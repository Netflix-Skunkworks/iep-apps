/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.aggregator

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import com.fasterxml.jackson.core.JsonFactory
import com.netflix.atlas.akka.ByteStringInputStream
import com.netflix.atlas.core.util.RefIntHashMap
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Tag
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class UpdateApiSuite extends FunSuite {

  private val system = ActorSystem("UpdateApiSuite")
  private val factory = new JsonFactory()

  private val aggrTag = Tag.of("atlas.dstype", "sum")

  private def createAggrService(clock: Clock): AtlasAggregatorService = {
    new AtlasAggregatorService(ConfigFactory.load(), clock, new NoopRegistry, system)
  }

  test("simple payload") {
    val clock = new ManualClock()
    val service = createAggrService(clock)
    val parser = factory.createParser("""
        |[
        |  2,
        |  "name",
        |  "cpu",
        |  1,
        |  0, 1,
        |  0,
        |  42.0
        |]
      """.stripMargin)
    assertEquals(UpdateApi.processPayload(parser, service).status, StatusCodes.OK)
    clock.setWallTime(62000)
    val id = Id.create("cpu").withTag(aggrTag)
    assertEquals(service.lookup(id).counter(id).actualCount(), 42.0)
  }

  test("payload with additional tags") {
    val clock = new ManualClock()
    val service = createAggrService(clock)
    val parser = factory.createParser("""
        |[
        |  6,
        |  "name",
        |  "cpu",
        |  "app",
        |  "www",
        |  "zone",
        |  "1e",
        |  3,
        |  0, 1, 2, 3, 4, 5,
        |  0,
        |  42.0
        |]
      """.stripMargin)
    assertEquals(UpdateApi.processPayload(parser, service).status, StatusCodes.OK)
    clock.setWallTime(62000)
    val id = Id
      .create("cpu")
      .withTags("app", "www", "zone", "1e")
      .withTag(aggrTag)
    assertEquals(service.lookup(id).counter(id).actualCount(), 42.0)
  }

  test("payload with invalid characters") {
    val clock = new ManualClock()
    val service = createAggrService(clock)
    val parser = factory.createParser("""
        |[
        |  6,
        |  "name",
        |  "cpu user",
        |  "app",
        |  "www",
        |  "zone",
        |  "1e",
        |  3,
        |  0, 1, 2, 3, 4, 5,
        |  0,
        |  42.0
        |]
      """.stripMargin)
    assertEquals(UpdateApi.processPayload(parser, service).status, StatusCodes.OK)
    clock.setWallTime(62000)
    val id = Id
      .create("cpu_user")
      .withTags("app", "www", "zone", "1e")
      .withTag(aggrTag)
    assertEquals(service.lookup(id).counter(id).actualCount(), 42.0)
  }

  test("payload with invalid characters null") {
    val clock = new ManualClock()
    val service = createAggrService(clock)
    val json = Json.encode(
      List(
        6,
        "name",
        "cpu\u0000user",
        "app",
        "www",
        "zone",
        "1e",
        3,
        0,
        1,
        2,
        3,
        4,
        5,
        0,
        42.0
      )
    )
    val parser = factory.createParser(json)
    assertEquals(UpdateApi.processPayload(parser, service).status, StatusCodes.OK)
    clock.setWallTime(62000)
    val id = Id
      .create("cpu_user")
      .withTags("app", "www", "zone", "1e")
      .withTag(aggrTag)
    assertEquals(service.lookup(id).counter(id).actualCount(), 42.0)
  }

  test("percentile node rollup") {
    val clock = new ManualClock()
    val service = createAggrService(clock)
    val parser = factory.createParser("""
        |[
        |  7,
        |  "name",
        |  "latency",
        |  "percentile",
        |  "T0000",
        |  "nf.node",
        |  "i-12345",
        |  "nf.task",
        |  4,
        |  0, 1, 2, 3, 4, 5, 6, 5,
        |  0,
        |  42.0
        |]
      """.stripMargin)
    assertEquals(UpdateApi.processPayload(parser, service).status, StatusCodes.OK)
    clock.setWallTime(62000)
    val id = Id
      .create("latency")
      .withTag(aggrTag)
      .withTag("percentile", "T0000")
    assertEquals(service.lookup(id).counter(id).actualCount(), 42.0)
  }

  private def createPayload(ts: List[TagMap], op: Int, value: Double): String = {
    val stringTable = new RefIntHashMap[String]()
    ts.foreach { tags =>
      tags.foreachEntry { (k, v) =>
        stringTable.put(k, 0)
        stringTable.put(v, 0)
      }
    }
    val strings = new Array[String](stringTable.size)
    var i = 0
    stringTable.foreach { (k, _) =>
      strings(i) = k
      i += 1
    }
    java.util.Arrays.sort(strings.asInstanceOf[Array[AnyRef]])

    val data = List.newBuilder[Any]
    data.addOne(strings.length)
    strings.zipWithIndex.foreach {
      case (s, i) =>
        data.addOne(s)
        stringTable.put(s, i)
    }
    var offset = 0
    ts.foreach { tags =>
      data.addOne(tags.size)
      tags.foreachEntry { (k, v) =>
        data.addOne(stringTable.get(k, -1))
        data.addOne(stringTable.get(v, -1))
      }
      data.addOne(op)
      data.addOne(value)
      offset += tags.size * 2
    }
    Json.encode(data)
  }

  private def validationTest(tags: TagMap, expectedStatus: StatusCode): FailureMessage = {
    validationTest(List(tags), expectedStatus)
  }

  private def validationTest(ts: List[TagMap], expectedStatus: StatusCode): FailureMessage = {
    val clock = new ManualClock()
    val service = createAggrService(clock)
    val parser = factory.createParser(createPayload(ts, 0, 1.0))
    val response = UpdateApi.processPayload(parser, service)
    assertEquals(response.status, expectedStatus)
    assert(response.entity.isStrict())
    val strict = response.entity.asInstanceOf[HttpEntity.Strict]
    Json.decode[FailureMessage](new ByteStringInputStream(strict.data))
  }

  test("validation: ok") {
    val tags = SmallHashMap("name" -> "foo")
    validationTest(tags, StatusCodes.OK)
  }

  test("validation: missing name") {
    val tags = SmallHashMap("foo" -> "bar")
    val msg = validationTest(tags, StatusCodes.BadRequest)
    assertEquals(msg.errorCount, 1)
    assertEquals(msg.message, List("missing key 'name' (tags={\"foo\":\"bar\"})"))
  }

  test("validation: too many user tags") {
    val tags = Map("name" -> "foo") ++ (0 until 20)
        .map(v => Strings.zeroPad(v, 5))
        .map(v => v -> v)
    val msg = validationTest(SmallHashMap(tags), StatusCodes.BadRequest)
    assertEquals(msg.errorCount, 1)
    assert(msg.message.head.startsWith("too many user tags: 21 > 20 (tags={"))
  }

  test("validation: user tags, ignore restricted") {
    val tags = Map("name" -> "foo", "nf.app" -> "www") ++ (0 until 19)
        .map(v => Strings.zeroPad(v, 5))
        .map(v => v -> v)
    val msg = validationTest(SmallHashMap(tags), StatusCodes.OK)
    assertEquals(msg.errorCount, 0)
  }

  test("validation: tag rule") {
    val tags = SmallHashMap("name" -> "test", "nf.foo" -> "bar")
    val msg = validationTest(tags, StatusCodes.BadRequest)
    assertEquals(msg.errorCount, 1)
    assertEquals(
      msg.message,
      List(
        "invalid key for reserved prefix 'nf.': nf.foo (tags={\"name\":\"test\",\"nf.foo\":\"bar\"})"
      )
    )
  }

  test("validation: partial failure") {
    val ts = List(
      SmallHashMap("name" -> "test", "nf.foo" -> "bar"),
      SmallHashMap("name" -> "test", "nf.app" -> "bar")
    )
    val msg = validationTest(ts, StatusCodes.Accepted)
    assertEquals(msg.errorCount, 1)
    assertEquals(
      msg.message,
      List(
        "invalid key for reserved prefix 'nf.': nf.foo (tags={\"name\":\"test\",\"nf.foo\":\"bar\"})"
      )
    )
  }

  test("validation: truncate if there are too many errors") {
    val ts = (0 until 20).toList.map { i =>
      SmallHashMap("name" -> i.toString, "nf.foo" -> "bar")
    }
    val msg = validationTest(ts, StatusCodes.BadRequest)
    assertEquals(msg.errorCount, 20)
    assertEquals(msg.message.size, 5)
  }
}
