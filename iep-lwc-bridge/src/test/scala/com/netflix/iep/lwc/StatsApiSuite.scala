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
package com.netflix.iep.lwc

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.akka.testkit.MUnitRouteSuite
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.atlas.impl.Subscription
import com.typesafe.config.ConfigFactory

class StatsApiSuite extends MUnitRouteSuite {

  import scala.jdk.CollectionConverters.*
  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val config = ConfigFactory.load()
  private val evaluator = new ExpressionsEvaluator(config, new NoopRegistry)
  private val endpoint = new StatsApi(evaluator)
  private val routes = RequestHandler.standardOptions(endpoint.routes)

  private def assertJsonContentType(response: HttpResponse): Unit = {
    assertEquals(response.entity.contentType.mediaType, MediaTypes.`application/json`)
  }

  private def assertResponse(response: HttpResponse, expected: StatusCode): Unit = {
    assertEquals(response.status, expected)
    assertJsonContentType(response)
  }

  override def beforeEach(context: BeforeEach): Unit = {
    evaluator.clear()
  }

  test("empty") {
    Get("/api/v1/stats") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      assertEquals(responseAs[String], "[]")
    }
  }

  test("has data") {
    // Add sample subscription
    val subs = List(
      new Subscription()
        .withId("1")
        .withExpression("name,ssCpuUser,:eq,:sum"),
      new Subscription()
        .withId("2")
        .withExpression("name,ssCpuSystem,:eq,:sum")
    )
    evaluator.sync(subs.asJava)

    // Stats only get updated when data is sent
    val datapoints = List(
      new BridgeDatapoint("ssCpuUser", Array("node", "i-1"), 2, 60000, 42.0),
      new BridgeDatapoint("ssCpuUser", Array("node", "i-2"), 2, 60000, 44.0)
    )
    evaluator.eval(60000, datapoints)

    // Query the data
    Get("/api/v1/stats") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val stats = Json.decode[List[ExpressionsEvaluator.SubscriptionStats]](responseAs[String])
      assertEquals(stats.length, 1)
      assertEquals(stats.head.updateCount.get(), 2L)
    }
  }
}
