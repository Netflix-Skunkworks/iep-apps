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
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.atlas.AtlasConfig
import com.netflix.spectator.atlas.AtlasRegistry
import org.scalatest.FunSuite

class UpdateApiSuite extends FunSuite {

  private val factory = new JsonFactory()

  test("simple payload") {
    val clock = new ManualClock()
    val registry = new AtlasRegistry(clock, UpdateApiSuite.config)
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
    UpdateApi.processPayload(parser, registry)
    clock.setWallTime(62000)
    assert(registry.counter(registry.createId("cpu")).actualCount() === 42.0)
  }

  test("payload with additional tags") {
    val clock = new ManualClock()
    val registry = new AtlasRegistry(clock, UpdateApiSuite.config)
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
    UpdateApi.processPayload(parser, registry)
    clock.setWallTime(62000)
    val id = registry.createId("cpu", "app", "www", "zone", "1e")
    assert(registry.counter(id).actualCount() === 42.0)
  }

  test("payload with invalid characters") {
    val clock = new ManualClock()
    val registry = new AtlasRegistry(clock, UpdateApiSuite.config)
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
    UpdateApi.processPayload(parser, registry)
    clock.setWallTime(62000)
    val id = registry.createId("cpu_user", "app", "www", "zone", "1e")
    assert(registry.counter(id).actualCount() === 42.0)
  }
}

object UpdateApiSuite {
  private val config = new AtlasConfig {
    override def get(k: String): String = null
  }
}
