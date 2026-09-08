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

import java.net.ConnectException
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.graph.DefaultSettings
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.pekko.CallerContext
import com.netflix.atlas.pekko.Principal
import com.netflix.atlas.webapi.GraphApi.DataRequest
import com.netflix.atlas.webapi.GraphApi.DataResponse
import com.netflix.atlas.webapi.TagsApi.ListValuesRequest
import com.netflix.atlas.webapi.TagsApi.ValueListResponse
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.util.Failure
import scala.util.Success

/**
  * Checks that the identity of the caller established for an incoming request makes it all the
  * way to the requests sent to druid. The unit tests for `DruidClient` only cover the header
  * being attached when a token is passed in explicitly, they would not notice if the actor
  * stopped extracting the token from the request.
  */
class DruidCallerPropagationSuite extends FunSuite {

  import DruidClient.*
  import DruidDatabaseActor.*

  private val config = ConfigFactory.load()

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private implicit val timeout: Timeout = Timeout(10.seconds)

  private val caller = CallerContext(
    Principal(Principal.Kind.App, "atlas-druid"),
    Principal(Principal.Kind.User, "someuser"),
    Some("e2e-token-value")
  )

  private val metadata = Metadata(
    List(
      DatasourceMetadata("ds_1", Datasource(List("a", "b"), List(Metric("m1", "LONG"))))
    )
  )

  /**
    * Actor wired to a client that records the requests sent to druid. The metadata refresh
    * triggered on startup uses a GET, it is failed so it cannot overwrite the metadata that
    * is pushed in by the test.
    */
  private def newActor(requests: ConcurrentLinkedQueue[HttpRequest]): ActorRef = {
    val client = Flow[(HttpRequest, AccessLogger)]
      .map {
        case (req, logger) =>
          requests.add(req)
          val result =
            if (req.method == HttpMethods.GET)
              Failure(new ConnectException("metadata refresh disabled for test"))
            else
              Success(emptyArrayResponse)
          result -> logger
      }
    val druidClient = new DruidClient(config.getConfig("atlas.druid"), system, client)
    val ref =
      system.actorOf(Props(new DruidDatabaseActor(config, new DruidMetadataService, druidClient)))
    ref ! metadata
    ref
  }

  private def emptyArrayResponse: HttpResponse = {
    HttpResponse(StatusCodes.OK, entity = "[]".getBytes(StandardCharsets.UTF_8))
  }

  /** Tokens seen on the query requests, the metadata GET requests are not caller specific. */
  private def queryTokens(requests: ConcurrentLinkedQueue[HttpRequest]): List[Option[String]] = {
    requests.asScala.toList
      .filter(_.method == HttpMethods.POST)
      .map(_.getHeader(e2eTokenHeaderName).toScala.map(_.value))
  }

  private def toDataRequest(uri: String, caller: CallerContext): DataRequest = {
    val grapher = Grapher(DefaultSettings(config))
    DataRequest(grapher.toGraphConfig(HttpRequest(uri = Uri(uri)), caller))
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  test("data request relays the e2e token to druid") {
    val requests = new ConcurrentLinkedQueue[HttpRequest]
    val ref = newActor(requests)
    val request = toDataRequest("/api/v1/graph?q=name,m1,:eq,:sum", caller)
    Await.result((ref ? request).mapTo[DataResponse], Duration.Inf)
    assertEquals(queryTokens(requests), List(Some("e2e-token-value")))
  }

  test("data request without a token does not set the header") {
    val requests = new ConcurrentLinkedQueue[HttpRequest]
    val ref = newActor(requests)
    val request = toDataRequest("/api/v1/graph?q=name,m1,:eq,:sum", CallerContext.Anonymous)
    Await.result((ref ? request).mapTo[DataResponse], Duration.Inf)
    assertEquals(queryTokens(requests), List(None))
  }

  test("list values request relays the e2e token to druid") {
    val requests = new ConcurrentLinkedQueue[HttpRequest]
    val ref = newActor(requests)
    val tq = TagQuery(Some(Query.Equal("name", "m1")), Some("a"))
    Await.result((ref ? ListValuesRequest(tq, caller)).mapTo[ValueListResponse], Duration.Inf)
    assertEquals(queryTokens(requests), List(Some("e2e-token-value")))
  }

  test("list values request without a token does not set the header") {
    val requests = new ConcurrentLinkedQueue[HttpRequest]
    val ref = newActor(requests)
    val tq = TagQuery(Some(Query.Equal("name", "m1")), Some("a"))
    Await.result((ref ? ListValuesRequest(tq)).mapTo[ValueListResponse], Duration.Inf)
    assertEquals(queryTokens(requests), List(None))
  }
}
