/*
 * Copyright 2014-2021 Netflix, Inc.
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
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Using

class ExprUpdateServiceSuite extends AnyFunSuite with BeforeAndAfter {

  private implicit val system = ActorSystem()

  private val config = ConfigFactory.load()
  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)
  private val evaluator = new ExpressionsEvaluator(config, registry)

  private val service = new ExprUpdateService(config, registry, evaluator, system)

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

  private def gzip(input: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    Using.resource(new GZIPOutputStream(baos)) { out =>
      out.write(input.getBytes(StandardCharsets.UTF_8))
    }
    baos.toByteArray
  }

  private def doValidUpdate(compress: Boolean = false): Unit = {
    val json = {
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
    }
    if (compress)
      update(HttpResponse(StatusCodes.OK, entity = gzip(json)))
    else
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
    assert(1 === evaluator.index.findMatches(Id.create("cpu")).size)
  }

  test("valid update refreshes age metric") {
    doValidUpdate()
    clock.setWallTime(60000)
    doValidUpdate()
    PolledMeter.update(registry)
    val age = registry.gauge("lwc.expressionsAge").value()
    assert(age === 0.0)
  }

  test("valid update compressed") {
    doValidUpdate(true)
    assert(1 === evaluator.index.findMatches(Id.create("cpu")).size)
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
