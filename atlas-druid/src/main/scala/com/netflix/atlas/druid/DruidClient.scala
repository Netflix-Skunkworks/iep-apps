/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.json.Json
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.util.Failure
import scala.util.Success

class DruidClient(
  config: Config,
  system: ActorSystem,
  materializer: ActorMaterializer,
  client: HttpClient
) extends StrictLogging {

  import DruidClient._

  private val uri = config.getString("uri")

  private implicit val mat = materializer

  private val loggingClient = Flow[HttpRequest]
    .map(req => req -> AccessLogger.newClientLogger("druid", req))
    .via(client)
    .map {
      case (result, logger) =>
        logger.complete(result)
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

  private def mkRequest[T: Manifest](data: T): HttpRequest = {
    val json = Json.encode(data)
    logger.trace(s"raw request payload: $json")
    val entity = HttpEntity(MediaTypes.`application/json`, json)
    HttpRequest(HttpMethods.POST, uri, entity = entity)
  }

  def datasources: Source[List[String], NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, s"$uri/datasources")
    Source
      .single(request)
      .via(loggingClient)
      .map(data => Json.decode[List[String]](data.toArray))
  }

  def datasource(name: String): Source[Datasource, NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, s"$uri/datasources/$name")
    Source
      .single(request)
      .via(loggingClient)
      .map(data => Json.decode[Datasource](data.toArray))
  }

  def segmentMetadata(query: SegmentMetadataQuery): Source[List[SegmentMetadataResult], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map(data => Json.decode[List[SegmentMetadataResult]](data.toArray))
  }

  def search(query: SearchQuery): Source[List[SearchResult], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map(data => Json.decode[List[SearchResult]](data.toArray))
  }

  def groupBy(query: GroupByQuery): Source[List[GroupByDatapoint], NotUsed] = {
    Source
      .single(mkRequest(query))
      .via(loggingClient)
      .map(data => Json.decode[List[GroupByDatapoint]](data.toArray))
      .map(_.filter(_.isValid))
  }
}

object DruidClient {

  case class Datasource(dimensions: List[String], metrics: List[Metric])

  case class Metric(name: String, dataType: String) {
    def isCounter: Boolean = dataType == "FLOAT" || dataType == "LONG"
    def isHistogram: Boolean = dataType == "netflixHistogram"
    def isSupported: Boolean = isCounter
  }

  // http://druid.io/docs/latest/querying/segmentmetadataquery.html
  case class SegmentMetadataQuery(
    dataSource: String,
    intervals: List[String] = null,
    toInclude: Option[ToInclude] = None,
    merge: Boolean = true,
    analysisTypes: List[String] = List("cardinality", "size", "aggregators"),
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
    dataSource: String,
    searchDimensions: List[DimensionSpec],
    intervals: List[String],
    filter: Option[DruidFilter] = None,
    sort: DruidSort = DruidSort.Lexicographic,
    granularity: String = "all",
    limit: Int = 1000,
  ) {
    val queryType: String = "search"
    val query: DruidQuery.type = DruidQuery
  }

  case object DruidQuery {
    val `type`: String = "regex"
    val pattern: String = ".*"
  }

  case class SearchResult(
    timestamp: String,
    result: List[DimensionValue]
  ) {
    def values: List[String] = result.map(_.value).distinct.sorted
  }

  case class DimensionValue(dimension: String, value: String, count: Int)

  // http://druid.io/docs/latest/querying/groupbyquery.html
  case class GroupByQuery(
    dataSource: String,
    dimensions: List[DimensionSpec],
    intervals: List[String],
    aggregations: List[Aggregation],
    filter: Option[DruidFilter] = None,
    granularity: Granularity = Granularity.millis(60000),
  ) {
    val queryType: String = "groupBy"
  }

  sealed trait DimensionSpec

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
  }

  case class RegexFilteredDimensionSpec(
    delegate: DimensionSpec,
    pattern: String
  ) extends DimensionSpec {
    val `type`: String = "regexFiltered"
  }

  case class Aggregation(`type`: String, fieldName: String) {
    val name: String = "value"
  }

  case object Aggregation {
    def count(fieldName: String): Aggregation = Aggregation("count", fieldName)
    def sum(fieldName: String): Aggregation = Aggregation("doubleSum", fieldName)
    def min(fieldName: String): Aggregation = Aggregation("doubleMin", fieldName)
    def max(fieldName: String): Aggregation = Aggregation("doubleMax", fieldName)
  }

  case class Granularity(duration: Long) {
    val `type`: String = "duration"
  }

  case object Granularity {
    def millis(amount: Long): Granularity = Granularity(amount)
    def fromDuration(dur: Duration): Granularity = Granularity(dur.toMillis)
  }

  case class GroupByDatapoint(timestamp: String, event: Map[String, String]) {

    /**
      * Checks that all values in the event are non-null. Druid treats empty strings and
      * null values as being the same. Some older threads suggest server side filtering
      * for null values may not be reliable. This could be fixed now, but as it is a fairly
      * rare occurrence in our use-cases and unlikely to have a big performance benefit, we
      * do a client side filtering to remove entries with null values.
      */
    def isValid: Boolean = event.forall(_._2 != null)
  }

  case class Event(tags: Map[String, String], value: Double)
}
