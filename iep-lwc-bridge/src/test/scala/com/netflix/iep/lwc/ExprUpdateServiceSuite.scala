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
package com.netflix.iep.lwc

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExprUpdateServiceSuite extends AnyFunSuite with BeforeAndAfter {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  private val config = ConfigFactory.load()
  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)
  private val evaluator = new ExpressionsEvaluator(config)

  private val service = new ExprUpdateService(config, registry, evaluator)

  private def update(response: HttpResponse): Unit = {
    val future = Source
      .single(response)
      .via(service.syncExpressionsFlow)
      .runWith(Sink.head)
    Await.ready(future, Duration.Inf)
  }

  before {
    clock.setWallTime(0)
    evaluator.clear()
  }

  private def doValidUpdate(): Unit = {
    val json =
      """
        |{
        |  "expressions": [
        |    {
        |      "id": "123",
        |      "expression": "name,cpu,:eq,:sum",
        |      "frequency": 60000
        |    },
        |    {
        |      "id": "123",
        |      "expression": "name,cpu,:eq,:sum",
        |      "frequency": 5000
        |    }
        |  ]
        |}
      """.stripMargin
    update(HttpResponse(StatusCodes.OK, entity = json))
  }

  private def doInvalidUpdate(): Unit = {
    val json =
      """
        |{
        |  "expressions": [
        |    {
        |      "id": "123",
        |      "expression": "name,cpu,:eq,:su",
        |      "frequency": 60000
        |    }
        |  ]
        |}
      """.stripMargin
    update(HttpResponse(StatusCodes.OK, entity = json))
  }

  test("valid update rebuilds index") {
    doValidUpdate()
    assert(1 === evaluator.index.matchingEntries(Map("name" -> "cpu")).size)
  }

  test("valid update refreshes age metric") {
    doValidUpdate()
    clock.setWallTime(60000)
    doValidUpdate()
    PolledMeter.update(registry)
    val age = registry.gauge("lwc.expressionsAge").value()
    assert(age === 0.0)
  }

  test("invalid expression does not refresh age metric") {
    doValidUpdate()
    clock.setWallTime(60000)
    doInvalidUpdate()
    PolledMeter.update(registry)
    val age = registry.gauge("lwc.expressionsAge").value()
    assert(age === 60.0)
  }
}
