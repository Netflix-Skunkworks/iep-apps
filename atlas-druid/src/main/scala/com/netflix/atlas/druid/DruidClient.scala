/*
 * Copyright 2014-2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.druid

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.EntityStreamSizeException
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import tools.jackson.core.JsonToken
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.ObjectNode
import tools.jackson.module.scala.JavaTypeable
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.pekko.ByteStringInputStream
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json3.Json
import com.netflix.atlas.json3.JsonParserHelper
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.InputStream
import java.util.Locale
import java.util.zip.GZIPInputStream
import scala.util.Failure
import scala.util.Success
import scala.util.Using

class DruidClient(
  config: Config,
  system: ActorSystem,
  client: HttpClient
) extends StrictLogging {

  import DruidClient.*

  private val uri = config.getString("uri")

  private implicit val mat: Materializer = Materializer(system)

  private val loggingClient = Flow[HttpRequest]
    .map(req => req -> AccessLogger.newClientLogger("druid", req))
    .via(client)
    .map {
      case (result, log) =>
        log.complete(result)
        result
    }
    .flatMapConcat {
      case Success(res) if isOK(res) => Source.single(res)
      case Success(res)              => Source.failed(fail(res))
      case Failure(t)                => Source.failed[HttpResponse](t)
    }
    .flatMapConcat { res =>
      res.entity.dataBytes.fold(ByteString.empty)(_ ++ _)
    }
    .map { data =>
      if (data.startsWith(gzipMagicHeader)) {
        logger.trace(s"raw response payload: GZip[${data.length}]")
      } else {
        logger.trace(s"raw response payload: ${data.decodeString(StandardCharsets.UTF_8)}")
      }
      data
    }
    .mapError {
      case e: EntityStreamSizeException =>
        new IllegalStateException(s"druid response size exceeds limit of ${e.limit} bytes", e)
    }

  private def isOK(res: HttpResponse): Boolean = res.status == StatusCodes.OK

  private def fail(res: HttpResponse): Throwable = {
    res.discardEntityBytes()
    new IOException(s"request failed with status ${res.status.intValue()}")
  }

  private def mkRequest(data: Any): HttpRequest = {
    val json = Json.encode(data)
    logger.trace(s"raw request payload: $json")
    val entity = HttpEntity(MediaTypes.`application/json`, json)
    val headers = List(`Accept-Encoding`(HttpEncodings.gzip))
    HttpRequest(HttpMethods.POST, uri, headers = headers, entity = entity)
  }

  def datasources: Source[List[String], NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, s"$uri/datasources")
    Source
      .single(request)
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[List[String]](in)
        }
      }
  }

  def datasource(name: String): Source[Datasource, NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, s"$uri/datasources/$name")
    Source
      .single(request)
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[Datasource](in)
        }
      }
  }

  def segmentMetadata(
    query: SegmentMetadataQuery,
    ignoreFailures: Boolean
  ): Source[List[SegmentMetadataResult], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[List[SegmentMetadataResult]](in)
        }
      }
      .recover {
        case t: Throwable =>
          val json = Json.encode(query)
          if (ignoreFailures) {
            logger.warn(s"failed to load segment metadata, ignoring data source: $json", t)
            Nil
          } else {
            logger.warn(s"failed to load segment metadata: $json")
            throw t
          }
      }
  }

  def search(query: SearchQuery): Source[List[SearchResult], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[List[SearchResult]](in)
        }
      }
  }

  def topn(query: TopNQuery): Source[List[TopNResult], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[List[TopNResult]](in)
        }
      }
  }

  def groupBy(query: GroupByQuery): Source[List[GroupByDatapoint], NotUsed] = {
    val dimensions = query.dimensions.map(_.outputName)
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map(data => parseResult(dimensions, valueDecoder(query), data))
  }

  def timeseries(query: TimeseriesQuery): Source[List[GroupByDatapoint], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[List[TimeseriesDatapoint]](in)
        }
      }
      .map(_.map(_.toGroupByDatapoint))
  }

  def data(query: DataQuery): Source[List[GroupByDatapoint], NotUsed] = {
    query match {
      case q: GroupByQuery    => groupBy(q)
      case q: TimeseriesQuery => timeseries(q)
    }
  }

  /**
    * Parse a data query response, folding each datapoint into `consumer` as it is decoded rather
    * than collecting the datapoints into a `List`, and emit the populated consumer so the caller
    * reads its result from the stream. The consumer is expected to fold into a compact
    * representation (e.g. one array per output series), which for a wide group by with many time
    * buckets bounds peak memory by the size of that result rather than by the response size.
    */
  def parseDatapoints[C <: DatapointConsumer](query: DataQuery)(consumer: C): Source[C, NotUsed] = {
    query match {
      case q: GroupByQuery =>
        val dimensions = q.dimensions.map(_.outputName)
        Source
          .single(mkRequest(q))
          .via(loggingClient)
          .map { data =>
            decodeGroupBy(dimensions, valueDecoder(q), data)(consumer)
            consumer
          }
      case q: TimeseriesQuery =>
        Source
          .single(mkRequest(q))
          .via(loggingClient)
          .map { data =>
            Using
              .resource(inputStream(data)) { in =>
                Json.decode[List[TimeseriesDatapoint]](in)
              }
              .foreach(d => consumer.accept(d.timestampMillis, Map.empty, d.result.value))
            consumer
          }
    }
  }

  private def valueDecoder(query: GroupByQuery): ValueDecoder = {
    if (query.aggregations.exists(_.aggrType == Aggregation.DistinctRegisterType))
      ValueDecoder.DistinctRegisters
    else if (query.aggregations.exists(_.aggrType == Aggregation.TimerType))
      ValueDecoder.TimerHistogram
    else
      ValueDecoder.Default
  }

  private def parseResult(
    dimensions: List[String],
    decoder: ValueDecoder,
    data: ByteString
  ): List[GroupByDatapoint] = {
    val builder = List.newBuilder[GroupByDatapoint]
    decodeGroupBy(dimensions, decoder, data) { (timestamp, tags, value) =>
      builder += GroupByDatapoint(timestamp, tags, value)
    }
    builder.result()
  }

  private def decodeGroupBy(
    dimensions: List[String],
    decoder: ValueDecoder,
    data: ByteString
  )(consumer: DatapointConsumer): Unit = {
    Using.resource(Json.newJsonParser(inputStream(data))) { parser =>
      import com.netflix.atlas.json3.JsonParserHelper.*
      foreachItem(parser) {
        require(parser.currentToken() == JsonToken.START_ARRAY)
        val timestamp = nextLong(parser)

        // Check that all values in the event are non-null. Druid treats empty strings and
        // null values as being the same. Some older threads suggest server side filtering
        // for null values may not be reliable. This could be fixed now, but as it is a fairly
        // rare occurrence in our use-cases and unlikely to have a big performance benefit, we
        // do a client side filtering to remove entries with null values. The tag map is built
        // directly to avoid the intermediate list of pairs allocated per datapoint.
        val tagsBuilder = Map.newBuilder[String, String]
        dimensions.foreach { d =>
          val v = parser.nextStringValue()
          if (!isNullOrEmpty(v)) tagsBuilder += d -> v
        }
        val tags = tagsBuilder.result()

        val valueToken = parser.nextToken()
        if (valueToken == JsonToken.START_OBJECT) {
          // Histogram type: {"bucketIndex": count, ...}
          val timer = decoder == ValueDecoder.TimerHistogram
          foreachField(parser) { idx =>
            val key = toPercentileBucket(idx, timer)
            val datapointTags = tags + (TagKey.percentile -> key)
            consumer.accept(timestamp, datapointTags, nextLong(parser).toDouble)
          }
        } else if (decoder == ValueDecoder.DistinctRegisters) {
          // Unfinalized HLL sketch, base64 encoded. Expand into one datapoint per register so
          // it looks the same as a sketch published by a Spectator DistinctCountSketch.
          if (valueToken != JsonToken.VALUE_NULL) {
            val values = HllSketchRegisters.decode(parser.getString)
            var i = 0
            while (i < values.length) {
              // Registers that were never set contribute nothing to the merge or the estimate,
              // so they are dropped rather than published as a series of zeros.
              if (values(i) > 0) {
                val datapointTags = tags + (TagKey.distinct -> HllSketchRegisters.tagValues(i))
                consumer.accept(timestamp, datapointTags, values(i).toDouble)
              }
              i += 1
            }
          }
        } else if (valueToken != JsonToken.VALUE_NULL) {
          // Floating point value. In some cases histogram can be null, ignore those entries.
          import tools.jackson.core.JsonToken.*
          val value = valueToken match {
            case VALUE_NUMBER_INT   => parser.getValueAsLong.toDouble
            case VALUE_NUMBER_FLOAT => parser.getValueAsDouble
            case VALUE_STRING       => java.lang.Double.parseDouble(parser.getString)
            case t => JsonParserHelper.fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
          }
          consumer.accept(timestamp, tags, value)
        }
        parser.nextToken() // skip end array token
      }
    }
  }

  private def toPercentileBucket(s: String, timer: Boolean): String = {
    val hex = Integer.toHexString(Integer.parseInt(s)).toUpperCase(Locale.US)
    val prefix = if (timer) "T" else "D"
    s"$prefix${Strings.zeroPad(hex, 4)}"
  }

  private def isNullOrEmpty(v: String): Boolean = v == null || v.isEmpty
}

object DruidClient {

  /**
    * How to interpret the value returned for a datapoint. Most types are a simple number, but
    * some are a compound value that needs to be expanded into a set of datapoints with an
    * additional dimension. Only the cases that cannot be told apart from the response alone
    * are listed: a histogram is recognized by the value being an object, so the decoder only
    * has to say whether those buckets are timer or distribution summary buckets.
    */
  private[druid] enum ValueDecoder {

    /** Histogram buckets for a timer, expanded into `percentile` datapoints. */
    case TimerHistogram

    /**
      * A simple numeric value, or histogram buckets for a distribution summary expanded into
      * `percentile` datapoints. Which one it is comes from the response, not the query.
      */
    case Default

    /** Serialized HLL sketch, expanded into `distinct` register datapoints. */
    case DistinctRegisters
  }

  case class Datasource(dimensions: List[String], metrics: List[Metric])

  case class Metric(name: String, dataType: String = "doubleSum", primaryStep: Long = 60000L) {

    def isSketch: Boolean = {
      dataType == "HLLSketchMerge"
    }

    def isCounter: Boolean =
      dataType == "doubleSum" || dataType == "floatSum" || dataType == "longSum" || isSketch

    def isMinMax: Boolean =
      dataType == "doubleMin" || dataType == "floatMin" || dataType == "longMin" ||
      dataType == "doubleMax" || dataType == "floatMax" || dataType == "longMax"

    def isTimer: Boolean = {
      dataType == "spectatorHistogramTimer"
    }

    def isDistSummary: Boolean = {
      dataType == "spectatorHistogram" || dataType == "spectatorHistogramDistribution"
    }

    def isHistogram: Boolean = isTimer || isDistSummary

    def isSupported: Boolean = isCounter || isHistogram || isMinMax
  }

  // http://druid.io/docs/latest/querying/segmentmetadataquery.html
  case class SegmentMetadataQuery(
    dataSource: String,
    intervals: List[String] = null,
    toInclude: Option[ToInclude] = None,
    merge: Boolean = true,
    analysisTypes: List[String] = List("aggregators", "queryGranularity"),
    aggregatorMergeStrategy: String = "latest"
  ) {
    val queryType: String = "segmentMetadata"
  }

  case class ToInclude(`type`: String, columns: List[String] = Nil)

  object ToInclude {

    def all: ToInclude = ToInclude("all")
    def none: ToInclude = ToInclude("none")
    def list(columns: List[String]): ToInclude = ToInclude("list", columns)
  }

  case object ToIncludeAll {
    val `type`: String = "all"
  }

  case class SegmentMetadataResult(
    id: String,
    intervals: List[String] = Nil,
    columns: Map[String, Column] = Map.empty,
    aggregators: Map[String, Aggregator] = Map.empty,
    queryGranularity: JsonNode,
    size: Long = 0L,
    numRows: Long = 0L
  ) {

    def toDatasource: Datasource = {
      val step = stepSize
      val dimensions = columns.filter(c => c._2 != null && c._2.isDimension).keys.toList.sorted
      val metrics = aggregators
        .filterNot(_._2 == null)
        .map {
          case (name, column) => Metric(name, column.`type`, step)
        }
        .filter(_.isSupported)
        .toList
      Datasource(dimensions, metrics)
    }

    def stepSize: Long = {
      val defaultStep = 60000
      // https://druid.apache.org/docs/latest/querying/granularities
      queryGranularity match {
        case g if g.isString =>
          // none is up to millisecond, use 1s. For all others treat as 1m
          g.stringValue().toLowerCase(Locale.US) match {
            case "none" | "second" => 1000
            case _                 => defaultStep
          }
        case g: ObjectNode =>
          g.get("type").stringValue() match {
            case "duration" => g.get("duration").asLong()
            case _          => defaultStep
          }
        case _ =>
          defaultStep
      }
    }
  }

  case class Column(
    `type`: String,
    hasMultipleValues: Boolean,
    size: Long,
    cardinality: Long,
    errorMessage: String
  ) {

    def isDimension: Boolean = `type` == "STRING" && !isError
    def isMetric: Boolean = !isDimension && !isError
    def isError: Boolean = errorMessage != null
  }

  case class Aggregator(
    `type`: String,
    name: String,
    fieldName: String
  )

  // http://druid.io/docs/latest/querying/searchquery.html
  case class SearchQuery(
    private val dataSources: List[String],
    searchDimensions: List[DimensionSpec],
    intervals: List[String],
    filter: Option[DruidFilter] = None,
    sort: DruidSort = DruidSort.Lexicographic,
    granularity: String = "all",
    limit: Int = 1000
  ) {

    val queryType: String = "search"
    // Use a union of the datasource(s) to send 1 query to Druid
    // The Druid broker will handle sending the query to each datasource
    // and merge the results before responding.
    // https://druid.apache.org/docs/latest/querying/query-execution.html#union
    val dataSource: UnionDatasource = UnionDatasource(dataSources)
  }

  case class UnionDatasource(dataSources: List[String]) {
    val `type`: String = "union"
  }

  case class SearchResult(
    timestamp: String,
    result: List[DimensionValue]
  ) {
    def values: List[String] = result.map(_.value).distinct.sorted
  }

  case class DimensionValue(dimension: String, value: String, count: Int)

  // https://druid.apache.org/docs/latest/querying/topnquery.html
  case class TopNQuery(
    dataSource: String,
    dimension: DimensionSpec,
    intervals: List[String],
    filter: Option[DruidFilter] = None,
    granularity: String = "all",
    metric: TopNMetricSpec = TopNMetricSpec.Dimension,
    threshold: Int = 1000
  ) {

    val queryType: String = "topN"
  }

  trait TopNMetricSpec

  object TopNMetricSpec {

    case object Dimension extends TopNMetricSpec {

      val `type`: String = "dimension"
      val ordering: DruidSort = DruidSort.Lexicographic
      val previousStop: Option[String] = None
    }

    case class Numeric(
      metric: String
    ) extends TopNMetricSpec {
      val `type`: String = "numeric"
    }

    case class Inverted(
      metric: TopNMetricSpec
    ) extends TopNMetricSpec {
      val `type`: String = "inverted"
    }
  }

  case class TopNResult(
    timestamp: String,
    result: List[TopNDimensionValue]
  ) {
    def values: List[String] = result.map(_.value).filterNot(v => v == null || v.isEmpty)
  }

  case class TopNDimensionValue(value: String)

  sealed trait DataQuery {

    /** Returns a new query with the additional context merged in. */
    def withAdditionalContext(ctxt: Map[String, Any]): DataQuery
  }

  // http://druid.io/docs/latest/querying/groupbyquery.html
  // https://druid.apache.org/docs/latest/querying/groupbyquery.html#array-based-result-rows
  // https://github.com/apache/druid/issues/8118
  case class GroupByQuery(
    dataSource: String,
    dimensions: List[DimensionSpec],
    intervals: List[String],
    aggregations: List[Aggregation],
    filter: Option[DruidFilter] = None,
    having: Option[HavingSpec] = None,
    granularity: Granularity = Granularity.millis(60000),
    context: Map[String, Any] = Map("resultAsArray" -> true)
  ) extends DataQuery {

    val queryType: String = "groupBy"

    def toTimeseriesQuery: TimeseriesQuery = {
      require(dimensions.isEmpty)
      // The timeseries response format only has a place for a simple numeric value, so
      // aggregations that expand into an additional dimension cannot be decoded from it.
      require(
        !aggregations.exists(Aggregation.expandsToDimension),
        "aggregation expands into an additional dimension, group by is required"
      )
      TimeseriesQuery(dataSource, intervals, aggregations, filter, having, granularity)
    }

    override def withAdditionalContext(ctxt: Map[String, Any]): GroupByQuery = {
      copy(context = context ++ ctxt)
    }
  }

  // http://druid.io/docs/latest/querying/timeseries.html
  case class TimeseriesQuery(
    dataSource: String,
    intervals: List[String],
    aggregations: List[Aggregation],
    filter: Option[DruidFilter] = None,
    having: Option[HavingSpec] = None,
    granularity: Granularity = Granularity.millis(60000),
    context: Map[String, Any] = Map.empty
  ) extends DataQuery {

    val queryType: String = "timeseries"

    override def withAdditionalContext(ctxt: Map[String, Any]): TimeseriesQuery = {
      copy(context = context ++ ctxt)
    }
  }

  sealed trait DimensionSpec {
    def outputName: String
  }

  case class DefaultDimensionSpec(dimension: String, outputName: String) extends DimensionSpec {

    val `type`: String = "default"
    val outputType: String = "STRING"
  }

  case class ListFilteredDimensionSpec(
    delegate: DimensionSpec,
    values: List[String],
    isWhitelist: Boolean = true
  ) extends DimensionSpec {

    val `type`: String = "listFiltered"

    override def outputName: String = delegate.outputName
  }

  case class RegexFilteredDimensionSpec(
    delegate: DimensionSpec,
    pattern: String
  ) extends DimensionSpec {

    val `type`: String = "regexFiltered"

    override def outputName: String = delegate.outputName
  }

  @JsonIgnoreProperties(Array("aggrType"))
  @JsonInclude(JsonInclude.Include.NON_NULL)
  case class Aggregation(
    aggrType: String,
    fieldName: String,
    lgK: Integer = null,
    tgtHllType: String = null,
    shouldFinalize: java.lang.Boolean = null
  ) {

    // Type to encode for Druid request. Internally we need to distinguish between timers
    // and distribution summaries, but the Druid aggregation type is the same for both.
    val `type`: String = aggrType match {
      case Aggregation.TimerType            => "spectatorHistogram"
      case Aggregation.DistSummaryType      => "spectatorHistogram"
      case Aggregation.DistinctRegisterType => "HLLSketchMerge"
      case _                                => aggrType
    }

    val name: String = "value"
  }

  case object Aggregation {

    // Internal aggregation types, used to distinguish cases where the druid type alone is
    // not enough to know how the response value should be decoded. These are not sent to
    // druid, see the `type` mapping above.
    private[druid] val TimerType: String = "timer"
    private[druid] val DistSummaryType: String = "dist-summary"
    private[druid] val DistinctRegisterType: String = "distinct-register"

    /**
      * Types where the value returned by druid expands into datapoints with an additional
      * dimension rather than being a simple number. Those responses can only be decoded from
      * the group by result format.
      */
    def expandsToDimension(aggr: Aggregation): Boolean = {
      aggr.aggrType == TimerType ||
      aggr.aggrType == DistSummaryType ||
      aggr.aggrType == DistinctRegisterType
    }

    def count(fieldName: String): Aggregation = Aggregation("count", fieldName)
    def sum(fieldName: String): Aggregation = Aggregation("doubleSum", fieldName)
    def min(fieldName: String): Aggregation = Aggregation("doubleMin", fieldName)
    def max(fieldName: String): Aggregation = Aggregation("doubleMax", fieldName)
    def distinct(fieldName: String): Aggregation = Aggregation("HLLSketchMerge", fieldName)
    def timer(fieldName: String): Aggregation = Aggregation(TimerType, fieldName)
    def distSummary(fieldName: String): Aggregation = Aggregation(DistSummaryType, fieldName)

    /**
      * Aggregation for retrieving the raw registers of a native Druid HLL sketch rather than
      * the finalized distinct count. Druid rescales the stored sketch to the register count
      * used by the Spectator `DistinctCountSketch`, so the registers can be merged with those
      * published by any other source. Disabling finalization is what makes Druid return the
      * merged sketch, base64 encoded, in place of the estimate.
      */
    def distinctRegisters(fieldName: String): Aggregation = {
      Aggregation(
        aggrType = DistinctRegisterType,
        fieldName = fieldName,
        lgK = Integer.numberOfTrailingZeros(HllSketchRegisters.registers),
        tgtHllType = "HLL_4",
        shouldFinalize = java.lang.Boolean.FALSE
      )
    }
  }

  /**
    * For now it is limited to simple greater than filters to exclude 0 values.
    * https://druid.apache.org/docs/latest/querying/having.html
    */
  case class HavingSpec(aggregation: String, value: Double) {
    val `type`: String = "greaterThan"
  }

  case class Granularity(duration: Long) {
    val `type`: String = "duration"
  }

  case object Granularity {

    def millis(amount: Long): Granularity = Granularity(amount)
    def fromDuration(dur: Duration): Granularity = Granularity(dur.toMillis)
  }

  case class GroupByDatapoint(timestamp: Long, tags: Map[String, String], value: Double)

  /**
    * Consumer invoked for each datapoint as a data query response is parsed. The timestamp and
    * value are passed as primitives rather than wrapped in a `GroupByDatapoint` so that callers
    * folding the stream avoid both the wrapper allocation and the boxing a generic function would
    * incur on the per-datapoint hot path.
    */
  trait DatapointConsumer {
    def accept(timestamp: Long, tags: Map[String, String], value: Double): Unit
  }

  case class TimeseriesDatapoint(timestamp: String, result: TimeseriesResult) {

    def timestampMillis: Long = Instant.parse(timestamp).toEpochMilli

    def toGroupByDatapoint: GroupByDatapoint = {
      GroupByDatapoint(timestampMillis, Map.empty, result.value)
    }
  }

  case class TimeseriesResult(value: Double)

  // Magic header to recognize GZIP compressed data
  // http://www.zlib.org/rfc-gzip.html#file-format
  private val gzipMagicHeader = ByteString(Array(0x1F.toByte, 0x8B.toByte))

  /**
    * Create an InputStream for reading the content of the ByteString. If the data is
    * gzip compressed, then it will be wrapped in a GZIPInputStream to handle the
    * decompression of the data. This can be handled at the server layer, but it may
    * be preferable to decompress while parsing into the final object model to reduce
    * the need to allocate an intermediate ByteString of the uncompressed data.
    */
  private def inputStream(bytes: ByteString): InputStream = {
    if (bytes.startsWith(gzipMagicHeader))
      new GZIPInputStream(new ByteStringInputStream(bytes))
    else
      new ByteStringInputStream(bytes)
  }
}
