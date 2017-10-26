/*
 * Copyright 2014-2017 Netflix, Inc.
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

import scala.util.Failure
import scala.util.Success

class DruidClient(config: Config, system: ActorSystem, materializer: ActorMaterializer, client: HttpClient) {

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

  private def isOK(res: HttpResponse): Boolean = res.status == StatusCodes.OK

  private def fail(res: HttpResponse): Throwable = {
    res.discardEntityBytes()
    new IOException(s"request failed with status ${res.status.intValue()}")
  }

  private def mkRequest[T: Manifest](data: T): HttpRequest = {
    val json = Json.encode(data)
    val entity = HttpEntity(MediaTypes.`application/json`, json)
    HttpRequest(HttpMethods.POST, uri, entity = entity)
  }

  def datasources: Source[List[String], NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, s"$uri/datasources")
    Source.single(request)
      .via(loggingClient)
      .map(data => Json.decode[List[String]](data.toArray))
  }

  def datasource(name: String): Source[Datasource, NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, s"$uri/datasources/$name")
    Source.single(request)
      .via(loggingClient)
      .map(data => Json.decode[Datasource](data.toArray))
  }

  def search(query: SearchQuery): Source[List[SearchResult], NotUsed] = {
    Source.single(mkRequest(query))
      .via(loggingClient)
      .map(data => Json.decode[List[SearchResult]](data.toArray))
  }

  def groupBy(query: GroupByQuery): Source[List[GroupByDatapoint], NotUsed] = {
    Source.single(mkRequest(query))
      .via(loggingClient)
      .map(data => Json.decode[List[GroupByDatapoint]](data.toArray))
  }
}

object DruidClient {

  case class Datasource(dimensions: List[String], metrics: List[String])

  // http://druid.io/docs/latest/querying/searchquery.html
  case class SearchQuery(
    dataSource: String,
    searchDimensions: List[String],
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
    dimensions: List[String],
    intervals: List[String],
    aggregations: List[Aggregation],
    filter: Option[DruidFilter] = None,
    granularity: Granularity = Granularity.millis(60000),
  ) {
    val queryType: String = "groupBy"
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

  case class GroupByDatapoint(timestamp: String, event: Map[String, String])

  case class Event(tags: Map[String, String], value: Double)
}
