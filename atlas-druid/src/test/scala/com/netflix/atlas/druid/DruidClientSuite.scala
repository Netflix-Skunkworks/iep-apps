/*
 * Copyright 2014-2020 Netflix, Inc.
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
import java.net.ConnectException
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.fasterxml.jackson.databind.JsonMappingException
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.json.Json
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class DruidClientSuite extends FunSuite with BeforeAndAfterAll {

  import DruidClient._

  private val config = ConfigFactory.load().getConfig("atlas.druid")

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  private def newClient(result: Try[HttpResponse]): DruidClient = {
    val client = Flow[(HttpRequest, AccessLogger)]
      .map {
        case (_, logger) => result -> logger
      }
    new DruidClient(config, system, materializer, client)
  }

  private def ok[T: Manifest](data: T): HttpResponse = {
    val json = Json.encode(data).getBytes(StandardCharsets.UTF_8)
    HttpResponse(StatusCodes.OK, entity = json)
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  test("get datasources") {
    val client = newClient(Success(ok(List("a", "b", "c"))))
    val future = client.datasources.runWith(Sink.head)
    val result = Await.result(future, Duration.Inf)
    assert(result === List("a", "b", "c"))
  }

  test("get datasources http error") {
    intercept[IOException] {
      val client = newClient(Success(HttpResponse(StatusCodes.BadRequest)))
      val future = client.datasources.runWith(Sink.head)
      Await.result(future, Duration.Inf)
    }
  }

  test("get datasources connect timeout") {
    intercept[ConnectException] {
      val client = newClient(Failure(new ConnectException("failed")))
      val future = client.datasources.runWith(Sink.head)
      Await.result(future, Duration.Inf)
    }
  }

  test("get datasources read failure") {
    intercept[IOException] {
      val data = Source.failed[ByteString](new IOException("read failed"))
      val entity = HttpEntity(MediaTypes.`application/json`, data)
      val response = HttpResponse(StatusCodes.OK, entity = entity)
      val client = newClient(Success(response))
      val future = client.datasources.runWith(Sink.head)
      Await.result(future, Duration.Inf)
    }
  }

  test("get datasources bad json output") {
    intercept[JsonMappingException] {
      val json = """{"foo":"bar"}"""
      val data = Source.single[ByteString](ByteString(json))
      val entity = HttpEntity(MediaTypes.`application/json`, data)
      val response = HttpResponse(StatusCodes.OK, entity = entity)
      val client = newClient(Success(response))
      val future = client.datasources.runWith(Sink.head)
      Await.result(future, Duration.Inf)
    }
  }

  test("get datasource empty") {
    val client = newClient(Success(ok(Datasource(Nil, Nil))))
    val future = client.datasource("abc").runWith(Sink.head)
    val result = Await.result(future, Duration.Inf)
    assert(result === Datasource(Nil, Nil))
  }

  test("get datasource with data") {
    val ds = Datasource(List("a", "b"), List(Metric("m1", "LONG"), Metric("m2", "LONG")))
    val client = newClient(Success(ok(ds)))
    val future = client.datasource("abc").runWith(Sink.head)
    val result = Await.result(future, Duration.Inf)
    assert(result === ds)
  }

  private def executeSegmentMetadataRequest: List[SegmentMetadataResult] = {
    import com.netflix.atlas.core.util.Streams._
    val file = "segmentMetadataResponse.json"
    val payload = scope(resource(file))(byteArray)
    val response = HttpResponse(StatusCodes.OK, entity = payload)
    val client = newClient(Success(response))
    val query = SegmentMetadataQuery("test")
    val future = client.segmentMetadata(query).runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("segmentMetadata columns") {
    val result = executeSegmentMetadataRequest
    assert(result.size === 1)

    val columns = result.head.columns
    assert(columns.size === 5)

    val expected = Set(
      "__time",
      "test.metric.counter",
      "test.dim.1",
      "test.dim.2",
      "test.metric.histogram"
    )
    assert(columns.keySet === expected)
  }

  test("segmentMetadata column types") {
    val columns = executeSegmentMetadataRequest.head.columns
    assert(columns("__time").`type` === "LONG")
    assert(columns("test.metric.counter").`type` === "LONG")
    assert(columns("test.metric.histogram").`type` === "netflixHistogram")
    assert(columns("test.dim.1").`type` === "STRING")
    assert(columns("test.dim.1").`type` === "STRING")
  }

  test("segmentMetadata aggregators") {
    val aggregators = executeSegmentMetadataRequest.head.aggregators
    assert(aggregators.size === 2)

    val expected = Set(
      "test.metric.counter",
      "test.metric.histogram"
    )
    assert(aggregators.keySet === expected)
  }

  private def executeGroupByRequest: List[GroupByDatapoint] = {
    import com.netflix.atlas.core.util.Streams._
    val file = "groupByResponse.json"
    val payload = scope(resource(file))(byteArray)
    val response = HttpResponse(StatusCodes.OK, entity = payload)
    val client = newClient(Success(response))
    val query = GroupByQuery("test", Nil, Nil, Nil)
    val future = client.groupBy(query).runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("groupBy filter out null dimensions") {
    val datapoints = executeGroupByRequest
    assert(datapoints.count(_.tags.isEmpty) === 1)
    assert(datapoints.count(_.tags.nonEmpty) === 5)
  }
}
