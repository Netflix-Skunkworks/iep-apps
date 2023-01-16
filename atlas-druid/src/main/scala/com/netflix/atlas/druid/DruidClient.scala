/*
 * Copyright 2014-2023 Netflix, Inc.
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
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.module.scala.JavaTypeable
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.ByteStringInputStream
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonParserHelper
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

  import DruidClient._

  private val uri = config.getString("uri")

  private implicit val mat = Materializer(system)

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
      res.entity
        .withoutSizeLimit()
        .dataBytes
        .fold(ByteString.empty)(_ ++ _)
    }
    .map { data =>
      logger.trace(s"raw response payload: ${data.decodeString(StandardCharsets.UTF_8)}")
      data
    }

  private def isOK(res: HttpResponse): Boolean = res.status == StatusCodes.OK

  private def fail(res: HttpResponse): Throwable = {
    res.discardEntityBytes()
    new IOException(s"request failed with status ${res.status.intValue()}")
  }

  private def mkRequest[T: JavaTypeable](data: T): HttpRequest = {
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

  def segmentMetadata(query: SegmentMetadataQuery): Source[List[SegmentMetadataResult], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map { data =>
        Using.resource(inputStream(data)) { in =>
          Json.decode[List[SegmentMetadataResult]](in)
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
    val timer = query.aggregations.exists(_.aggrType == "timer")
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map(data => parseResult(dimensions, timer, data))
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

  private def parseResult(
    dimensions: List[String],
    timer: Boolean,
    data: ByteString
  ): List[GroupByDatapoint] = {
    Using.resource(Json.newJsonParser(inputStream(data))) { parser =>
      import com.netflix.atlas.json.JsonParserHelper._
      val builder = List.newBuilder[GroupByDatapoint]
      foreachItem(parser) {
        require(parser.currentToken() == JsonToken.START_ARRAY)
        val timestamp = nextLong(parser)

        // Check that all values in the event are non-null. Druid treats empty strings and
        // null values as being the same. Some older threads suggest server side filtering
        // for null values may not be reliable. This could be fixed now, but as it is a fairly
        // rare occurrence in our use-cases and unlikely to have a big performance benefit, we
        // do a client side filtering to remove entries with null values.
        val tags = dimensions
          .map { d =>
            d -> parser.nextTextValue()
          }
          .filterNot(t => isNullOrEmpty(t._2))
          .toMap

        val valueToken = parser.nextToken()
        if (valueToken == JsonToken.START_OBJECT) {
          // Histogram type: {"bucketIndex": count, ...}
          foreachField(parser) { idx =>
            val key = toPercentileBucket(idx, timer)
            val datapointTags = tags + (TagKey.percentile -> key)
            builder += GroupByDatapoint(timestamp, datapointTags, nextLong(parser).toDouble)
          }
        } else if (valueToken != JsonToken.VALUE_NULL) {
          // Floating point value. In some cases histogram can be null, ignore those entries.
          import com.fasterxml.jackson.core.JsonToken._
          val value = valueToken match {
            case VALUE_NUMBER_INT   => parser.getValueAsLong.toDouble
            case VALUE_NUMBER_FLOAT => parser.getValueAsDouble
            case VALUE_STRING       => java.lang.Double.parseDouble(parser.getText)
            case t => JsonParserHelper.fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
          }
          builder += GroupByDatapoint(timestamp, tags, value)
        }
        parser.nextToken() // skip end array token
      }
      builder.result()
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

  case class Datasource(dimensions: List[String], metrics: List[Metric])

  case class Metric(name: String, dataType: String = "DOUBLE") {

    def isCounter: Boolean = dataType == "DOUBLE" || dataType == "FLOAT" || dataType == "LONG"

    def isTimer: Boolean = {
      dataType == "spectatorHistogramTimer"
    }

    def isDistSummary: Boolean = {
      dataType == "spectatorHistogram" || dataType == "spectatorHistogramDistribution"
    }

    def isHistogram: Boolean = isTimer || isDistSummary

    def isSupported: Boolean = isCounter || isHistogram
  }

  // http://druid.io/docs/latest/querying/segmentmetadataquery.html
  case class SegmentMetadataQuery(
    dataSource: String,
    intervals: List[String] = null,
    toInclude: Option[ToInclude] = None,
    merge: Boolean = true,
    analysisTypes: List[String] = List("aggregators"),
    lenientAggregatorMerge: Boolean = false
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
    size: Long = 0L,
    numRows: Long = 0L
  ) {

    def toDatasource: Datasource = {
      val dimensions = columns.filter(_._2.isDimension).keys.toList.sorted
      val metrics = columns
        .filter(_._1 != "__time")
        .filter(_._2.isMetric)
        .map {
          case (name, column) => Metric(name, column.`type`)
        }
        .filter(_.isSupported)
        .toList
      Datasource(dimensions, metrics)
    }
  }

  case class Column(
    `type`: String,
    hasMultipleValues: Boolean,
    size: Long,
    cardinality: Long
  ) {

    def isDimension: Boolean = `type` == "STRING"
    def isMetric: Boolean = !isDimension
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
    private val dataSources: List[String],
    dimension: DimensionSpec,
    intervals: List[String],
    filter: Option[DruidFilter] = None,
    granularity: String = "all",
    metric: TopNMetricSpec = TopNMetricSpec.Dimension,
    threshold: Int = 1000
  ) {

    val queryType: String = "topN"
    val dataSource: UnionDatasource = UnionDatasource(dataSources)
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
  case class Aggregation(aggrType: String, fieldName: String) {

    // Type to encode for Druid request. Internally we need to distinguish between timers
    // and distribution summaries, but the Druid aggregation type is the same for both.
    val `type`: String = aggrType match {
      case "timer"        => "spectatorHistogram"
      case "dist-summary" => "spectatorHistogram"
      case _              => aggrType
    }

    val name: String = "value"
  }

  case object Aggregation {

    def count(fieldName: String): Aggregation = Aggregation("count", fieldName)
    def sum(fieldName: String): Aggregation = Aggregation("doubleSum", fieldName)
    def min(fieldName: String): Aggregation = Aggregation("doubleMin", fieldName)
    def max(fieldName: String): Aggregation = Aggregation("doubleMax", fieldName)
    def timer(fieldName: String): Aggregation = Aggregation("timer", fieldName)
    def distSummary(fieldName: String): Aggregation = Aggregation("dist-summary", fieldName)
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

  case class TimeseriesDatapoint(timestamp: String, result: TimeseriesResult) {

    def toGroupByDatapoint: GroupByDatapoint = {
      val t = Instant.parse(timestamp).toEpochMilli
      GroupByDatapoint(t, Map.empty, result.value)
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
