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
package com.netflix.iep.lwc

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json.Json
import com.netflix.spectator.atlas.impl.Subscription
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite


class StatsApiSuite extends FunSuite with ScalatestRouteTest with BeforeAndAfter {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  private implicit val routeTestTimeout = RouteTestTimeout(5.second)

  private val evaluator = new ExpressionsEvaluator
  private val endpoint = new StatsApi(evaluator)
  private val routes = RequestHandler.standardOptions(endpoint.routes)

  private def assertJsonContentType(response: HttpResponse): Unit = {
    assert(response.entity.contentType.mediaType === MediaTypes.`application/json`)
  }

  private def assertResponse(response: HttpResponse, expected: StatusCode): Unit = {
    assert(response.status === expected)
    assertJsonContentType(response)
  }

  before {
    evaluator.clear()
  }

  test("empty") {
    Get("/api/v1/stats") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      assert(responseAs[String] === "[]")
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
      Datapoint(Map("name" -> "ssCpuUser", "node" -> "i-1"), 60000, 42.0),
      Datapoint(Map("name" -> "ssCpuUser", "node" -> "i-2"), 60000, 44.0)
    )
    evaluator.eval(60000, datapoints)

    // Query the data
    Get("/api/v1/stats") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val stats = Json.decode[List[ExpressionsEvaluator.SubscriptionStats]](responseAs[String])
      assert(stats.length === 1)
      assert(stats.head.updateCount.get() === 2)
    }
  }
}
