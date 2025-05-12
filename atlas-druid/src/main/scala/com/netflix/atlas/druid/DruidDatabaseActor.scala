/*
 * Copyright 2014-2025 Netflix, Inc.
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

import java.time.Instant
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.ConsolidationFunction
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.ResultSet
import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.model.Query.KeyQuery
import com.netflix.atlas.core.model.Query.KeyValueQuery
import com.netflix.atlas.core.model.Query.PatternQuery
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.ListHelper
import com.netflix.atlas.json.Json
import com.netflix.atlas.webapi.GraphApi.DataRequest
import com.netflix.spectator.impl.PatternMatcher
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success

class DruidDatabaseActor(config: Config, service: DruidMetadataService, client: DruidClient)
    extends Actor
    with StrictLogging {

  import DruidClient.*
  import DruidDatabaseActor.*

  import scala.concurrent.ExecutionContext.Implicits.global

  import ExplainApi.*
  import com.netflix.atlas.webapi.GraphApi.*
  import com.netflix.atlas.webapi.TagsApi.*

  private implicit val sys: ActorSystem = context.system

  private val maxDataSize = config.getBytes("atlas.druid.max-data-size")

  private val metadataInterval = config.getDuration("atlas.druid.metadata-interval")
  private val tagsInterval = config.getDuration("atlas.druid.tags-interval")

  private val datasourceFilter =
    PatternMatcher.compile(config.getString("atlas.druid.datasource-filter"))

  private val normalizeRates = config.getBoolean("atlas.druid.normalize-rates")

  private var metadata: Metadata = Metadata(Nil)

  private val cancellable =
    context.system.scheduler.scheduleAtFixedRate(0.seconds, 10.minutes, self, Tick)

  def receive: Receive = {
    case Tick => refreshMetadata(sender())
    case m: Metadata => {
      metadata = Metadata(m.datasources.filter(_.nonEmpty))
      sender() ! "metadata_updated"
    }
    case ListTagsRequest(tq)   => listValues(sendTags(sender()), tq)
    case ListKeysRequest(tq)   => listKeys(sender(), tq)
    case ListValuesRequest(tq) => listValues(sendValues(sender()), tq)
    case req: DataRequest      => fetchData(sender(), req)
    case req: ExplainRequest   => explain(sender(), req.dataRequest)
  }

  private def refreshMetadata(ref: ActorRef): Unit = {
    client.datasources
      .flatMapConcat { ds =>
        val sources = ds
          .filter(datasourceFilter.matches)
          .map { d =>
            client
              .segmentMetadata(SegmentMetadataQuery(d, List(queryIntervalString(metadataInterval))))
              .filter(_.nonEmpty)
              .map { rs =>
                DatasourceMetadata(d, rs.head.toDatasource)
              }
          }
        Source(sources)
      }
      .flatMapConcat(v => v)
      .fold(List.empty[DatasourceMetadata]) { (acc, v) =>
        v :: acc
      }
      .map { vs =>
        logger.trace(s"metadata: ${Json.encode(vs)}")
        Metadata(vs)
      }
      .runWith(Sink.head)
      .onComplete {
        case Success(m) =>
          logger.info("completed loading metadata")
          ref ! m
          service.metadataRefreshed()
        case Failure(t) =>
          logger.warn("failed to refresh metadata", t)
      }
  }

  private def listKeys(ref: ActorRef, tq: TagQuery): Unit = {
    val query = tq.query.getOrElse(Query.True)
    val vs = metadata.datasources
      .filter { ds =>
        ds.metricTags.exists(query.couldMatch)
      }
      .flatMap { ds =>
        ds.metricTags.flatMap(_.keys) ::: ds.datasource.dimensions
      }
      .filter(_ > tq.offset)
      .distinct
      .sorted
    ref ! KeyListResponse(vs)
  }

  type ListCallback = (String, List[String]) => Unit

  private def sendValues(ref: ActorRef): (String, List[String]) => Unit = { (_, vs) =>
    ref ! ValueListResponse(vs)
  }

  private def sendTags(ref: ActorRef): (String, List[String]) => Unit = { (k, vs) =>
    ref ! TagListResponse(vs.map(v => Tag(k, v)))
  }

  private def listValues(callback: ListCallback, tq: TagQuery): Unit = {
    tq.key.getOrElse("name") match {
      case "name"          => listNames(callback, tq)
      case "nf.datasource" => listDatasources(callback, tq)
      case "statistic"     => listStatistics(callback, tq)
      case _               => listDimension(callback, tq)
    }
  }

  private def getListQuery(tq: TagQuery): Query = {
    val userQuery = tq.query.getOrElse(Query.True)
    if (tq.offset == null || tq.offset == "")
      userQuery
    else
      userQuery.and(Query.GreaterThan(tq.key.getOrElse("name"), tq.offset))
  }

  private def listNames(callback: ListCallback, tq: TagQuery): Unit = {
    val query = getListQuery(tq)
    val vs = metadata.datasources
      .flatMap(_.metricTags)
      .filter(query.couldMatch)
      .map(m => m("name"))
      .distinct
      .sorted
      .take(tq.limit)
    callback("name", vs)
  }

  private def listDatasources(callback: ListCallback, tq: TagQuery): Unit = {
    val query = getListQuery(tq)
    val vs = metadata.datasources
      .flatMap(_.metricTags)
      .filter(query.couldMatch)
      .map(m => m("nf.datasource"))
      .distinct
      .sorted
      .take(tq.limit)
    callback("nf.datasource", vs)
  }

  private def listStatistics(callback: ListCallback, tq: TagQuery): Unit = {
    val query = getListQuery(tq)
    val vs = metadata.datasources
      .flatMap(_.metricTags)
      .filter(query.couldMatch)
      .flatMap(_.get("statistic"))
      .distinct
      .sorted
      .take(tq.limit)
    callback("statistic", vs)
  }

  private def listDimension(callback: ListCallback, tq: TagQuery): Unit = {
    val query = getListQuery(tq)
    tq.key match {
      case Some(k) =>
        val datasources = metadata.datasources.filter { ds =>
          ds.datasource.dimensions.contains(k) && ds.metricTags.exists(query.couldMatch)
        }

        if (datasources.nonEmpty) {
          val dimensionFilterMatches = Query
            .cnfList(query)
            .map(toInClause)
            .collect {
              case q: KeyValueQuery if q.k == k => q
              case q: PatternQuery if q.k == k  => q
            }
          val druidQueries = datasources
            .flatMap { ds =>
              // If there's a metric name in the query, ensure we simplify the query taking
              // into account the name match earlier so only the branch of the :or for the particular
              // metric will be present when constructing the filter
              val names = ds.metrics.filter(m => query.couldMatch(m.tags)).map(_.metric.name)

              val filterQuery = simplify(query, names, ds.datasource.dimensions)
              if (filterQuery == Query.False) None
              else {
                Some(
                  TopNQuery(
                    dataSource = ds.name,
                    dimension =
                      toDimensionSpec(DefaultDimensionSpec(k, "value"), dimensionFilterMatches),
                    intervals = List(queryIntervalString(tagsInterval)),
                    filter = DruidFilter.forQuery(filterQuery),
                    threshold = if (tq.limit < Int.MaxValue) tq.limit else 1000
                  )
                )
              }
            }

          if (druidQueries.nonEmpty) {
            Source(druidQueries.map { q => client.topn(q) })
              .flatMapMerge(Int.MaxValue, v => v)
              .map(_.flatMap(_.values))
              .fold(List.empty[String]) { (vs1, vs2) =>
                ListHelper.merge(tq.limit, vs1, vs2)
              }
              .runWith(Sink.foreach { vs =>
                callback(k, vs)
              })
          } else {
            callback(null, Nil)
          }
        } else {
          callback(null, Nil)
        }
      case None =>
        callback(null, Nil)
    }
  }

  private def queryIntervalString(duration: java.time.Duration): String = {
    val now = Instant.now().truncatedTo(ChronoUnit.MINUTES)
    s"${now.minus(duration)}/$now"
  }

  private def explain(ref: ActorRef, request: DataRequest): Unit = {
    val context = request.context
    val druidQueryContext = toDruidQueryContext(request)
    val explanation = request.exprs
      .flatMap { expr =>
        val offset = expr.offset.toMillis
        val fetchContext =
          if (offset == 0L) context
          else {
            context.copy(context.start - offset, context.end - offset)
          }
        val exprStr = expr.toString()
        toDruidQueries(metadata, druidQueryContext, fetchContext, expr)
          .map {
            case (tags, _, q) => Map("data-expr" -> exprStr, "tags" -> tags, "druid-query" -> q)
          }
      }
      .sortWith { (m1, m2) =>
        m1("data-expr").asInstanceOf[String] < m2("data-expr").asInstanceOf[String]
      }
    ref ! explanation
  }

  private def fetchData(ref: ActorRef, request: DataRequest): Unit = {
    try {
      request.exprs.foreach(expr => validate(expr.query))
      val druidQueryContext = toDruidQueryContext(request)
      val druidQueries = request.exprs.map { expr =>
        fetchData(druidQueryContext, request.context, expr).map(ts => expr -> ts)
      }
      Source(druidQueries)
        .flatMapMerge(Int.MaxValue, v => v)
        .fold(Map.empty[DataExpr, List[TimeSeries]]) { (acc, t) =>
          acc + t
        }
        .runWith(Sink.head)
        .onComplete {
          case Success(data) => ref ! DataResponse(data)
          case Failure(t)    => ref ! Failure(t)
        }
    } catch {
      case e: Exception => ref ! Failure(e)
    }
  }

  private def fetchData(
    druidQueryContext: Map[String, String],
    context: EvalContext,
    expr: DataExpr
  ): Source[List[TimeSeries], NotUsed] = {
    val offset = expr.offset.toMillis
    val fetchContext =
      if (offset == 0L) context
      else {
        context.copy(context.start - offset, context.end - offset)
      }
    val query = expr.query

    val druidQueries = toDruidQueries(metadata, druidQueryContext, fetchContext, expr).map {
      case (tags, metric, groupByQuery) =>
        // Always use the step set on the druid metric from the datasource metadata.
        // Ignore the "step" parameter on the request.
        val druidStep = metric.primaryStep
        val metricContext = withStep(fetchContext, druidStep)
        // For sketches just use the distinct count, other types are assumed to be counters.
        val valueMapper =
          if (metric.isSketch)
            (v: Double) => v
          else
            createValueMapper(normalizeRates, metricContext, expr)
        client.data(groupByQuery).map { result =>
          val candidates = toTimeSeries(tags, metricContext, result, maxDataSize, valueMapper)
          // See behavior on multi-value dimensions:
          // http://druid.io/docs/latest/querying/groupbyquery.html
          //
          // The queries sent to druid will include a filtering dimension spec to do as
          // much of the reduction server side as possible. However, it only supports list
          // and regex filters. Some queries, e.g., greater than, cannot be done with the
          // server side filter so we filter candidates locally to ensure the correct
          // results are included.
          val matchingSeries = candidates.filter(ts => query.couldMatch(ts.tags))

          // Ignore the step size on the query parameter
          // Always use the step size (granularity) from the metric's datasource in Druid.
          if (offset == 0L) matchingSeries else matchingSeries.map(_.offset(offset))
        }
    }

    // Filtering should have already been done at this point, just focus on the aggregation
    // behavior when evaluating the results. The query is simplified to exact matches that
    // are extracted to generate the labels. At this stage it is effectively true for matches.
    //
    // For :count, we rewrite to sum at this stage because the count will be computed on druid
    // and the final result should be summed.
    val evalExpr = expr
      .rewrite {
        case _: Query => simplifyExact(expr.query)
      }
      .rewrite {
        case af: DataExpr.Count => DataExpr.Sum(af.query, af.cf, af.offset)
      }
      .asInstanceOf[DataExpr]

    Source(druidQueries)
      .flatMapMerge(Int.MaxValue, v => v)
      .fold(List.empty[TimeSeries])(_ ::: _)
      .map { ts =>
        // All timeseries must have the same step at this point, if not the request would have already failed.
        // Ignore the "step" parameter on the request, use the step on the metrics.
        val step = ts.headOption.map(_.data.step).getOrElse(context.step)
        val druidContext = withStep(context, step)
        evalExpr.eval(druidContext, ts).data
      }
  }

  override def postStop(): Unit = {
    cancellable.cancel()
    super.postStop()
  }
}

object DruidDatabaseActor {

  import DruidClient.*

  case object Tick

  case class Metadata(datasources: List[DatasourceMetadata])

  case class DatasourceMetadata(name: String, datasource: Datasource) {

    val metrics: List[MetricMetadata] = datasource.metrics.map { metric =>
      val baseTags = Map("name" -> metric.name, "nf.datasource" -> name)
      val tags =
        if (metric.isHistogram)
          baseTags + ("statistic" -> "percentile")
        else
          baseTags
      MetricMetadata(metric, tags)
    }

    val metricTags: List[Map[String, String]] = metrics.map(_.tags)

    def nonEmpty: Boolean = datasource.metrics.nonEmpty
  }

  case class MetricMetadata(metric: DruidClient.Metric, tags: Map[String, String])

  private def withStep(ctx: EvalContext, step: Long): EvalContext = {
    if (step == ctx.step) {
      ctx
    } else {
      val s = ctx.start / step * step
      val e = ctx.end / step * step + step
      ctx.copy(start = s, end = e, step = step)
    }
  }

  def isSpecial(k: String): Boolean = {
    // statistic is included to support adding a percentile dimension for histogram
    // types. This will need to be refactored if we need to support explicit uses
    // in the future.
    //
    // percentile is included since it comes from the histogram type and isn't
    // available on druid. It will need to be handled in a post filter.
    k == "name" || k == "nf.datasource" || k == "statistic" || k == "percentile"
  }

  def toDimensionSpec(key: String, query: Query): DimensionSpec = {
    val matches = Query
      .cnfList(query)
      .map(toInClause)
      .collect {
        case q: KeyQuery if q.k == key => q
      }
    toDimensionSpec(DefaultDimensionSpec(key, key), matches)
  }

  private def getKey(query: Query): Option[String] = {
    query match {
      case q: Query.Equal             => Some(q.k)
      case q: Query.In                => Some(q.k)
      case Query.Not(Query.HasKey(k)) => Some(k)
      case _                          => None
    }
  }

  private def getValue(query: Query): List[String] = {
    query match {
      case q: Query.Equal             => List(q.v)
      case q: Query.In                => q.vs
      case Query.Not(Query.HasKey(_)) => List("")
      case _                          => Nil
    }
  }

  def toInClause(query: Query): Query = {
    val qs = Query.dnfList(query)
    val keys = qs.map(getKey).distinct
    keys match {
      case List(Some(k)) => Query.In(k, qs.flatMap(getValue).distinct)
      case _             => query
    }
  }

  @scala.annotation.tailrec
  private def toDimensionSpec(delegate: DimensionSpec, queries: List[Query]): DimensionSpec = {
    queries match {
      case Query.Equal(_, v) :: qs =>
        toDimensionSpec(ListFilteredDimensionSpec(delegate, List(v)), qs)
      case Query.In(_, vs) :: qs =>
        toDimensionSpec(ListFilteredDimensionSpec(delegate, vs), qs)
      case Query.Regex(_, p) :: qs =>
        toDimensionSpec(RegexFilteredDimensionSpec(delegate, s"^$p.*"), qs)
      case Query.RegexIgnoreCase(_, p) :: qs =>
        toDimensionSpec(RegexFilteredDimensionSpec(delegate, s"(?i)^$p.*"), qs)
      case _ :: qs =>
        toDimensionSpec(delegate, qs)
      case Nil =>
        delegate
    }
  }

  @scala.annotation.tailrec
  def toAggregation(metric: DruidClient.Metric, expr: DataExpr): Aggregation = {
    expr match {
      case _: DataExpr.All                      => throw new UnsupportedOperationException(":all")
      case Histogram(_) if metric.isTimer       => Aggregation.timer(metric.name)
      case Histogram(_) if metric.isDistSummary => Aggregation.distSummary(metric.name)
      case DataExpr.GroupBy(e, _)               => toAggregation(metric, e)
      case DataExpr.Consolidation(e, _)         => toAggregation(metric, e)
      case _ if metric.isSketch                 => Aggregation.distinct(metric.name)
      case _: DataExpr.Sum                      => Aggregation.sum(metric.name)
      case _: DataExpr.Max                      => Aggregation.max(metric.name)
      case _: DataExpr.Min                      => Aggregation.min(metric.name)
      case _: DataExpr.Count                    => Aggregation.count(metric.name)
    }
  }

  case object Histogram {

    def unapply(value: Any): Option[List[String]] = value match {
      case DataExpr.GroupBy(_: DataExpr.Sum, ks) if ks.contains("percentile") => Some(ks)
      case e: DataExpr if restrictsOnPercentile(e)                            => Some(Nil)
      case _                                                                  => None
    }

    private def restrictsOnPercentile(expr: DataExpr): Boolean = {
      restrictsOnPercentile(expr.query)
    }

    private def restrictsOnPercentile(query: Query): Boolean = query match {
      case Query.And(q1, q2) => restrictsOnPercentile(q1) || restrictsOnPercentile(q2)
      case Query.Or(q1, q2)  => restrictsOnPercentile(q1) || restrictsOnPercentile(q2)
      case Query.Not(q)      => restrictsOnPercentile(q)
      case q: Query.KeyQuery => q.k == "percentile"
      case _                 => false
    }
  }

  def toInterval(context: EvalContext): String = {
    val start = Instant.ofEpochMilli(context.start)
    val end = Instant.ofEpochMilli(context.end)
    s"$start/$end"
  }

  def createValueMapper(
    normalizeRates: Boolean,
    context: EvalContext,
    expr: DataExpr
  ): Double => Double = {

    if (isCount(expr)) {
      // Just use the raw value if count aggregation, druid will already have performed
      // the calculation.
      (v: Double) => v
    } else if (normalizeRates) {
      // Assume all values are counters that are in a rate per step. To make it consistent
      // with Atlas conventions, the value should be reported as a rate per second. This
      // may need to be revisited in the future if other types are supported.
      val stepSeconds = context.step / 1000.0
      (v: Double) => v / stepSeconds
    } else {
      expr.cf match {
        case ConsolidationFunction.Avg =>
          // If consolidating using average, divide by the multiple to preserve the unit
          // for the consolidated value.
          val multiple = context.step / DefaultSettings.stepSize
          (v: Double) => v / multiple
        case _ =>
          // For other consolidation functions like min, max, and sum, the value does not
          // need to be modified.
          (v: Double) => v
      }
    }
  }

  @scala.annotation.tailrec
  private def isCount(expr: DataExpr): Boolean = expr match {
    case by: DataExpr.GroupBy => isCount(by.af)
    case _: DataExpr.Count    => true
    case _                    => false
  }

  def toTimeSeries(
    commonTags: Map[String, String],
    context: EvalContext,
    data: List[GroupByDatapoint],
    limit: Long,
    valueMapper: Double => Double
  ): List[TimeSeries] = {
    val arrays = scala.collection.mutable.HashMap.empty[Map[String, String], Array[Double]]
    val length = context.bufferSize
    val bytesPerTimeSeries = 8L * length
    data.foreach { d =>
      val tags = d.tags
      val array = arrays.getOrElseUpdate(tags, ArrayHelper.fill(length, Double.NaN))
      val t = d.timestamp
      val i = ((t - context.start) / context.step).toInt
      if (i >= 0 && i < length) {
        array(i) = valueMapper(d.value)
      }
      // Stop early if data size is too large to avoid GC issues
      if (arrays.size * bytesPerTimeSeries > limit) {
        throw new IllegalStateException(s"data size exceeds $limit bytes")
      }
    }
    arrays.toList.flatMap {
      case (tags, vs) =>
        // Ignore entries with no non-NaN values, this sometimes occurs for groups if
        // there is data for neighboring time segments
        val seq = new ArrayTimeSeq(DsType.Rate, context.start + context.step, context.step, vs)
        val stats = SummaryStats(seq, context.start, context.end)
        if (stats.count == 0)
          None
        else
          Some(TimeSeries(commonTags ++ tags, seq))
    }
  }

  def toDruidQueryContext(request: DataRequest): Map[String, String] = {
    Map(
      "atlasQuerySource" -> request.config.map(_.id).getOrElse("unknown"),
      "atlasQueryString" -> request.config.map(_.query).getOrElse("unknown"),
      "atlasQueryGuid"   -> UUID.randomUUID().toString
    )
  }

  def toDruidQueries(
    metadata: Metadata,
    druidQueryContext: Map[String, String],
    context: EvalContext,
    expr: DataExpr
  ): List[(Map[String, String], DruidClient.Metric, DataQuery)] = {
    val query = expr.query
    val metrics = metadata.datasources.flatMap { ds =>
      ds.metrics.filter(m => query.couldMatch(m.tags)).map { m =>
        m -> ds.datasource.dimensions
      }
    }

    metrics.flatMap {
      case (m, ds) =>
        val name = m.tags("name")
        val datasource = m.tags("nf.datasource")

        val druidStep = m.metric.primaryStep
        val druidContext = withStep(context, druidStep)
        val intervals = List(toInterval(druidContext))

        // Common tags should be extracted for the simplified query rather than the raw
        // query. The simplified query may have additional exact matches due to simplified
        // OR clauses that need to be maintained for correct processing in the eval step.
        val simpleQuery = simplify(query, List(name), ds)
        val commonTags = exactTags(simpleQuery)
        val tags = commonTags ++ m.tags

        // Add has key checks for all keys within the group by. This allows druid to remove
        // some of the results earlier on in the historical nodes.
        val finalQuery = expr.finalGrouping
          .filter(k => !m.metric.isHistogram || k != TagKey.percentile)
          .filterNot(isSpecial)
          .map(k => Query.HasKey(k))
          .foldLeft(simpleQuery) { (q1, q2) =>
            q1.and(q2)
          }

        if (simpleQuery == Query.False) {
          None
        } else {
          val dimensions = getDimensions(simpleQuery, expr.finalGrouping, m.metric.isHistogram)

          // Add to the filter to remove rows that don't include the metric we're aggregating.
          // This reduces what needs to be merged and passed back to the broker, improving query
          // performance. This filter cannot be used with the spectatorHistogram type in druid.
          val metricValueFilter = Query.Not(Query.Equal(name, "0")).and(Query.HasKey(name))
          val finalQueryWithFilter =
            if (m.metric.isHistogram || m.metric.isSketch) finalQuery
            else finalQuery.and(metricValueFilter)

          val groupByQuery = GroupByQuery(
            dataSource = datasource,
            dimensions = dimensions,
            intervals = intervals,
            aggregations = List(toAggregation(m.metric, expr)),
            filter = DruidFilter.forQuery(finalQueryWithFilter),
            granularity = Granularity.millis(druidStep)
          )
          val druidQuery =
            if (groupByQuery.dimensions.isEmpty && !m.metric.isHistogram)
              groupByQuery.toTimeseriesQuery
            else
              groupByQuery
          Some((tags, m.metric, druidQuery.withAdditionalContext(druidQueryContext)))
        }
    }
  }

  def exactTags(query: Query): Map[String, String] = {
    query match {
      case Query.And(q1, q2) => exactTags(q1) ++ exactTags(q2)
      case Query.Equal(k, v) => Map(k -> v)
      case _                 => Map.empty
    }
  }

  def getDimensions(expr: DataExpr): List[DimensionSpec] = {
    getDimensions(expr.query, expr.finalGrouping)
  }

  def getDimensions(
    query: Query,
    groupByKeys: List[String],
    histogram: Boolean = false
  ): List[DimensionSpec] = {
    groupByKeys
      .filter(k => !histogram || k != TagKey.percentile)
      .filterNot(isSpecial)
      .map(k => toDimensionSpec(k, query))
  }

  /**
    * Simplify the query by mapping clauses for dimensions that are not present in the data source
    * to false.
    */
  private[druid] def simplify(query: Query, names: List[String], ds: List[String]): Query = {
    val simpleQuery = query match {
      case Query.And(q1, q2) => Query.And(simplify(q1, names, ds), simplify(q2, names, ds))
      case Query.Or(q1, q2)  => Query.Or(simplify(q1, names, ds), simplify(q2, names, ds))
      case Query.Not(q)      => Query.Not(simplify(q, names, ds))
      case q: KeyValueQuery if q.k == "name" => checkNameClause(q, names)
      case q: KeyQuery if q.k == "statistic" => Query.True
      case q: KeyQuery if isSpecial(q.k)     => q
      case q: KeyQuery if ds.contains(q.k)   => q
      case _: KeyQuery                       => Query.False
      case q: Query                          => q
    }
    Query.simplify(simpleQuery)
  }

  private def checkNameClause(q: KeyValueQuery, names: List[String]): Query = {
    if (names.exists(q.check)) Query.True else Query.False
  }

  /**
    * Simplify the query by keeping only clauses that have exact matches.
    */
  private[druid] def simplifyExact(query: Query): Query = {
    val simpleQuery = query match {
      case Query.And(q1, q2) => Query.And(simplifyExact(q1), simplifyExact(q2))
      case q: Query.Equal    => q
      case _: Query          => Query.True
    }
    Query.simplify(simpleQuery)
  }

  private[druid] def validate(query: Query): Unit = {
    Query.dnfList(query).foreach { q =>
      // Without a name clause, it can result in many broad druid queries that are quite
      // expensive. These vague queries can be common as intermediates when building a query,
      // but are not typically useful.
      require(hasNameClause(q), s"query does not restrict by name: $q")
    }
  }

  private def hasNameClause(query: Query): Boolean = query match {
    case Query.And(q1, q2) => hasNameClause(q1) || hasNameClause(q2)
    case q: Query.KeyQuery => q.k == "name"
    case _                 => false
  }
}
