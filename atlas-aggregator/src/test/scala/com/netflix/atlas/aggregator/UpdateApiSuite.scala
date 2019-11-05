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
package com.netflix.atlas.aggregator

import com.fasterxml.jackson.core.JsonFactory
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Tag
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class UpdateApiSuite extends FunSuite {

  private val factory = new JsonFactory()

  private val aggrTag = Tag.of("atlas.aggr", "i-123")

  private def createAggrService(clock: Clock): AtlasAggregatorService = {
    new AtlasAggregatorService(ConfigFactory.load(), clock, new NoopRegistry)
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
    UpdateApi.processPayload(parser, service)
    clock.setWallTime(62000)
    val id = Id.create("cpu").withTag(aggrTag)
    assert(service.lookup(id).counter(id).actualCount() === 42.0)
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
    UpdateApi.processPayload(parser, service)
    clock.setWallTime(62000)
    val id = Id
      .create("cpu")
      .withTags("app", "www", "zone", "1e")
      .withTag(aggrTag)
    assert(service.lookup(id).counter(id).actualCount() === 42.0)
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
    UpdateApi.processPayload(parser, service)
    clock.setWallTime(62000)
    val id = Id
      .create("cpu_user")
      .withTags("app", "www", "zone", "1e")
      .withTag(aggrTag)
    assert(service.lookup(id).counter(id).actualCount() === 42.0)
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
    UpdateApi.processPayload(parser, service)
    clock.setWallTime(62000)
    val id = Id
      .create("latency")
      .withTag(aggrTag)
      .withTag("percentile", "T0000")
    assert(service.lookup(id).counter(id).actualCount() === 42.0)
  }
}
