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

import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.graph.DefaultSettings
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.pekko.CallerContext
import com.netflix.atlas.webapi.GraphApi.DataRequest
import com.netflix.atlas.webapi.TagsApi.ListValuesRequest
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.*
import scala.util.Failure

/**
  * Checks that a druid request that times out is reported as a failure by the actor. The
  * Atlas APIs ask the actor with a fixed timeout, 30s for fetch, so the druid client needs
  * to fail first with the actual cause rather than racing the ask timeout.
  */
class DruidTimeoutSuite extends FunSuite {

  import DruidClient.*
  import DruidDatabaseActor.*

  // Use a short idle timeout so the test runs quickly. The production setting is
  // in the main application.conf.
  private val config = ConfigFactory
    .parseString("pekko.http.host-connection-pool.client.idle-timeout = 1s")
    .withFallback(ConfigFactory.load())
    .resolve()

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName, config)

  private val metadata = Metadata(
    List(
      DatasourceMetadata("ds_1", Datasource(List("a", "b"), List(Metric("m1", "LONG"))))
    )
  )

  // Timeout Atlas uses when asking the db actor for data on the fetch API, see
  // FetchRequestSource. It is not configurable.
  private val fetchAskTimeout = 30.seconds

  // Server that accepts query requests but never responds, similar to a slow druid query.
  // The metadata refresh triggered on startup uses a GET, it is failed right away so it
  // does not keep retrying against the hung server in the background.
  private val server = {
    val handler: HttpRequest => Future[HttpResponse] = { req =>
      if (req.method == HttpMethods.GET)
        Future.successful(HttpResponse(StatusCodes.ServiceUnavailable))
      else
        Promise[HttpResponse]().future
    }
    Await.result(Http().newServerAt("localhost", 0).bind(handler), 10.seconds)
  }

  private def newActor(): ActorRef = {
    val port = server.localAddress.getPort
    val druidConfig = ConfigFactory
      .parseString(s"""uri = "http://localhost:$port/druid/v2"""")
      .withFallback(config.getConfig("atlas.druid"))
    val client = Http().superPool[AccessLogger]()
    val druidClient = new DruidClient(druidConfig, system, client)
    val ref =
      system.actorOf(Props(new DruidDatabaseActor(config, new DruidMetadataService, druidClient)))
    ref ! metadata
    ref
  }

  private def toDataRequest(uri: String): DataRequest = {
    val grapher = Grapher(DefaultSettings(config))
    DataRequest(grapher.toGraphConfig(HttpRequest(uri = Uri(uri)), CallerContext.Anonymous))
  }

  override def afterAll(): Unit = {
    Await.result(server.unbind(), 10.seconds)
    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  test("production idle timeout fires before the fetch ask timeout") {
    // The suite config overrides the idle timeout, so check the shipped value separately. The
    // idle timeout is only checked once per second, so it can fire up to 1s late.
    val idleTimeout = ConfigFactory
      .load()
      .getDuration("pekko.http.host-connection-pool.client.idle-timeout")
      .toMillis
      .millis
    assert(idleTimeout + 1.second < fetchAskTimeout, s"idle-timeout = $idleTimeout")
  }

  /** Ask the actor and check that it replies with the idle timeout failure from the client. */
  private def assertTimeoutFailure(request: AnyRef): Unit = {
    val ref = newActor()
    val start = System.nanoTime()
    val response = Await.result(ref.ask(request)(Timeout(fetchAskTimeout)), Duration.Inf)
    val elapsed = (System.nanoTime() - start).nanos
    response match {
      case Failure(t) =>
        assert(t.getMessage.contains("idle-timeout"), t.toString)
      case r =>
        fail(s"expected timeout failure, got: $r")
    }
    assert(elapsed < 10.seconds, s"took $elapsed")
  }

  test("data request that times out replies with the timeout failure") {
    assertTimeoutFailure(toDataRequest("/api/v1/graph?q=name,m1,:eq,:sum"))
  }

  test("tag values request that times out replies with the timeout failure") {
    val tq = TagQuery(Some(Query.Equal("name", "m1")), Some("a"))
    assertTimeoutFailure(ListValuesRequest(tq, CallerContext.Anonymous))
  }
}
