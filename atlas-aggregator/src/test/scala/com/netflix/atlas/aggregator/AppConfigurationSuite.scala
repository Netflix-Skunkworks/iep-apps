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

import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import scala.util.Using

class AppConfigurationSuite extends FunSuite {

  private val config = ConfigFactory.load()

  test("aggr service") {
    Using.resource(new AnnotationConfigApplicationContext()) { context =>
      context.scan("com.netflix")
      context.refresh()
      context.start()
      assert(context.getBean(classOf[AtlasAggregatorService]) != null)
    }
  }

  test("aggr config should use prefix") {
    val config = ConfigFactory.parseString("""
        |netflix.atlas.aggr.registry.atlas.uri = "test"
      """.stripMargin)
    val aggr = new AggrConfig(config, new NoopRegistry, null)
    assertEquals(aggr.uri(), "test")
  }

  test("aggr config should use default for missing props") {
    val config = ConfigFactory.parseString("""
        |netflix.atlas.aggr.registry.atlas.uri = "test"
      """.stripMargin)
    val aggr = new AggrConfig(config, new NoopRegistry, null)
    assertEquals(aggr.batchSize(), 10000)
  }
}
