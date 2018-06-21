/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.NotUsed
import akka.actor.Actor
import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Query.KeyQuery
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.ListHelper
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

class DruidDatabaseActor(config: Config) extends Actor with StrictLogging {

  import DruidClient._
  import DruidDatabaseActor._

  import scala.concurrent.ExecutionContext.Implicits.global

  import com.netflix.atlas.webapi.GraphApi._
  import com.netflix.atlas.webapi.TagsApi._

  private implicit val sys = context.system
  private implicit val mat = ActorMaterializer()

  private val client = new DruidClient(
    config.getConfig("atlas.druid"), sys, mat, Http().superPool[AccessLogger]())

  private val tagsInterval = config.getDuration("atlas.druid.tags-interval")

  private var metadata: Metadata = Metadata(Nil)

  private val cancellable = context.system.scheduler.schedule(0.seconds, 10.minutes, self, Tick)

  def receive: Receive = {
    case Tick                  => refreshMetadata(sender())
    case m: Metadata           => metadata = Metadata(m.datasources.filter(_.nonEmpty))

    case ListTagsRequest(tq)   => listValues(sendTags(sender()), tq)
    case ListKeysRequest(tq)   => listKeys(sender(), tq)
    case ListValuesRequest(tq) => listValues(sendValues(sender()), tq)
    case req: DataRequest      => fetchData(sender(), req)
  }

  private def refreshMetadata(ref: ActorRef): Unit = {
    client.datasources
      .flatMapConcat { ds =>
        val sources = ds.map { d =>
          client.datasource(d).map(r => DatasourceMetadata(d, r))
        }
        Source(sources)
      }
      .flatMapConcat(v => v)
      .fold(List.empty[DatasourceMetadata]) { (acc, v) => v :: acc }
      .map(vs => Metadata(vs))
      .runWith(Sink.head)
      .onComplete {
        case Success(m) => ref ! m
        case Failure(t) => logger.warn("failed to refresh metadata", t)
      }
  }

  private def listKeys(ref: ActorRef, tq: TagQuery): Unit = {
    val query = tq.query.getOrElse(Query.True)
    val vs = metadata.datasources
      .filter { ds =>
        ds.metrics.exists(query.couldMatch)
      }
      .flatMap { ds =>
        "nf.datasource" :: "name" :: ds.datasource.dimensions
      }
      .distinct
      .sorted
    ref ! KeyListResponse(vs)
  }

  type ListCallback = (String, List[String]) => Unit

  private def sendValues(ref: ActorRef)(k: String, vs: List[String]): Unit = {
    ref ! ValueListResponse(vs)
  }

  private def sendTags(ref: ActorRef)(k: String, vs: List[String]): Unit = {
    ref ! TagListResponse(vs.map(v => Tag(k, v)))
  }

  private def listValues(callback: ListCallback, tq: TagQuery): Unit = {
    tq.key.getOrElse("name") match {
      case "name"          => listNames(callback, tq)
      case "nf.datasource" => listDatasources(callback, tq)
      case k               => listDimension(callback, tq)
    }
  }

  private def listNames(callback: ListCallback, tq: TagQuery): Unit = {
    val query = tq.query.getOrElse(Query.True)
    val vs = metadata.datasources
      .flatMap(_.metrics)
      .filter(query.couldMatch)
      .map(m => m("name"))
      .distinct
      .sorted
      .take(tq.limit)
    callback("name", vs)
  }

  private def listDatasources(callback: ListCallback, tq: TagQuery): Unit = {
    val query = tq.query.getOrElse(Query.True)
    val vs = metadata.datasources
      .flatMap(_.metrics)
      .filter(query.couldMatch)
      .map(m => m("nf.datasource"))
      .distinct
      .sorted
      .take(tq.limit)
    callback("nf.datasource", vs)
  }

  private def listDimension(callback: ListCallback, tq: TagQuery): Unit = {
    val query = tq.query.getOrElse(Query.True)
    tq.key match {
      case Some(k) =>
        val datasources = metadata.datasources.filter { ds =>
          ds.datasource.dimensions.contains(k) && ds.metrics.exists(query.couldMatch)
        }
        val druidQueries = datasources.map { ds =>
          val searchQuery = SearchQuery(
            dataSource = ds.name,
            searchDimensions = List(toDimensionSpec(k, query)),
            intervals = List(tagsQueryInterval),
            filter = DruidFilter.forQuery(query),
            limit = tq.limit
          )
          client.search(searchQuery)
        }
        Source(druidQueries)
          .flatMapMerge(Int.MaxValue, v => v)
          .map(_.flatMap(_.values))
          .fold(List.empty[String]) { (vs1, vs2) =>
            ListHelper.merge(tq.limit, vs1, vs2)
          }
          .runWith(Sink.foreach { vs =>
            callback(k, vs)
          })
      case None =>
        callback(null, Nil)
    }
  }

  private def tagsQueryInterval: String = {
    val now = Instant.now()
    s"${now.minus(tagsInterval)}/$now"
  }

  private def fetchData(ref: ActorRef, request: DataRequest): Unit = {
    val druidQueries = request.exprs.map { expr =>
      fetchData(request.context, expr).map(ts => expr -> ts)
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
  }

  private def fetchData(context: EvalContext, expr: DataExpr): Source[List[TimeSeries], NotUsed] = {
    val offset = expr.offset.toMillis
    val fetchContext = if (offset == 0L) context else {
      context.copy(context.start - offset, context.end - offset)
    }
    val query = expr.query
    val metrics = metadata.datasources.flatMap { ds =>
      ds.metrics.filter(query.couldMatch)
    }

    val commonTags = exactTags(query)

    val dimensions = expr match {
      case g: DataExpr.GroupBy => g.keys.filterNot(isSpecial).map(k => toDimensionSpec(k, query))
      case _                   => Nil
    }

    val intervals = List(toInterval(fetchContext))
    val druidQueries = metrics.map { m =>
      val name = m("name")
      val ds = m("nf.datasource")
      val tags = commonTags ++ m
      val groupByQuery = GroupByQuery(
        dataSource = ds,
        dimensions = dimensions,
        intervals = intervals,
        aggregations = List(toAggregation(name, expr)),
        filter = DruidFilter.forQuery(query),
        granularity = Granularity.millis(context.step)
      )
      client.groupBy(groupByQuery).map { result =>
        val candidates = toTimeSeries(tags, fetchContext, result)
        // See behavior on multi-value dimensions:
        // http://druid.io/docs/latest/querying/groupbyquery.html
        //
        // The queries sent to druid will include a filtering dimension spec to do as
        // much of the reduction server side as possible. However, it only supports list
        // and regex filters. Some queries, e.g., greater than, cannot be done with the
        // server side filter so we filter candidates locally to ensure the correct
        // results are included.
        val series = candidates.filter(ts => query.couldMatch(ts.tags))
        if (offset == 0L) series else series.map(_.offset(offset))
      }
    }

    Source(druidQueries)
      .flatMapMerge(Int.MaxValue, v => v)
      .fold(List.empty[TimeSeries])(_ ::: _)
      .map { ts => expr.eval(context, ts).data }
  }

  override def postStop(): Unit = {
    cancellable.cancel()
    super.postStop()
  }
}

object DruidDatabaseActor {
  import DruidClient._

  case object Tick

  case class Metadata(datasources: List[DatasourceMetadata])

  case class DatasourceMetadata(name: String, datasource: Datasource) {
    val metrics: List[Map[String, String]] = datasource.metrics.map { metric =>
      Map("name" -> metric, "nf.datasource" -> name)
    }

    def nonEmpty: Boolean = datasource.metrics.nonEmpty
  }

  def isSpecial(k: String): Boolean = {
    k == "name" || k == "nf.datasource"
  }

  def toDimensionSpec(key: String, query: Query): DimensionSpec = {
    val matches = Query.cnfList(query).collect {
      case q: KeyQuery if q.k == key => q
    }
    toDimensionSpec(DefaultDimensionSpec(key, key), matches)
  }

  private def toDimensionSpec(delegate: DimensionSpec, queries: List[Query]): DimensionSpec = {
    queries match {
      case Query.Equal(_, v) :: qs =>
        toDimensionSpec(ListFilteredDimensionSpec(delegate, List(v)), qs)
      case Query.In(_, vs) :: qs =>
        toDimensionSpec(ListFilteredDimensionSpec(delegate, vs), qs)
      case Query.Regex(_, p) :: qs =>
        toDimensionSpec(RegexFilteredDimensionSpec(delegate, s"^$p.*"), qs)
      case _ :: qs =>
        toDimensionSpec(delegate, qs)
      case Nil =>
        delegate
    }
  }

  def toAggregation(name: String, expr: DataExpr): Aggregation = {
    expr match {
      case _: DataExpr.All              => throw new UnsupportedOperationException(":all")
      case DataExpr.GroupBy(e, _)       => toAggregation(name, e)
      case DataExpr.Consolidation(e, _) => toAggregation(name, e)
      case e: DataExpr.Sum              => Aggregation.sum(name)
      case e: DataExpr.Max              => Aggregation.max(name)
      case e: DataExpr.Min              => Aggregation.min(name)
      case e: DataExpr.Count            => Aggregation.count(name)
    }
  }

  def toInterval(context: EvalContext): String = {
    val start = Instant.ofEpochMilli(context.start)
    val end = Instant.ofEpochMilli(context.end)
    s"$start/$end"
  }

  def toTimeSeries(commonTags: Map[String, String], context: EvalContext, data: List[GroupByDatapoint]): List[TimeSeries] = {
    val stepSeconds = context.step / 1000.0
    val arrays = scala.collection.mutable.AnyRefMap.empty[Map[String, String], Array[Double]]
    val length = context.bufferSize
    data.foreach { d =>
      val tags = d.event - "value"
      val array = arrays.getOrElseUpdate(tags, ArrayHelper.fill(length, Double.NaN))
      val t = Instant.parse(d.timestamp).toEpochMilli
      val i = ((t - context.start) / context.step).toInt
      if (i >= 0 && i < length) {
        // Assume all values are counters that are in a rate per step. To make it consistent
        // with Atlas conventions, the value should be reported as a rate per second. This
        // may need to be revisited in the future if other types are supported.
        array(i) = d.event.getOrElse("value", "NaN").toDouble / stepSeconds
      }
    }
    arrays.toList.map {
      case (tags, vs) =>
        val seq = new ArrayTimeSeq(DsType.Rate, context.start, context.step, vs)
        TimeSeries(commonTags ++ tags, seq)
    }
  }

  def exactTags(query: Query): Map[String, String] = {
    query match {
      case Query.And(q1, q2) => exactTags(q1) ++ exactTags(q2)
      case Query.Equal(k, v) => Map(k -> v)
      case _                 => Map.empty
    }
  }
}
