/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.core.model.Query
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import java.time.Instant

class MetricDefinitionSuite extends FunSuite {

  private val meta = MetricMetadata(
    MetricCategory("AWS/ELB", 60, -1, Nil, null, Nil, Some(Query.True)),
    null,
    Nil
  )

  test("bad config") {
    val cfg = ConfigFactory.empty()
    intercept[ConfigException] {
      MetricCategory.fromConfig(cfg)
    }
  }

  test("config with no tags") {
    val cfg = ConfigFactory.parseString("""
        |name = "RequestCount"
        |alias = "aws.elb.requests"
        |conversion = "sum,rate"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 1)
    assertEquals(definitions.head.name, "RequestCount")
    assertEquals(definitions.head.alias, "aws.elb.requests")
  }

  test("config with dsytpe rate") {
    val cfg = ConfigFactory.parseString("""
        |name = "RequestCount"
        |alias = "aws.elb.requests"
        |conversion = "sum,rate"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    definitions.head.conversion
    assertEquals(definitions.size, 1)
    assertEquals(definitions.head.tags, Map("atlas.dstype" -> "rate"))
  }

  test("config with dsytpe gauge") {
    val cfg = ConfigFactory.parseString("""
        |name = "RequestCount"
        |alias = "aws.elb.requests"
        |conversion = "sum"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 1)
    assertEquals(definitions.head.tags, Map("atlas.dstype" -> "gauge"))
  }

  test("config for timer") {
    val cfg = ConfigFactory.parseString("""
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "timer"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 3)
    assertEquals(definitions.map(_.tags("statistic")).toSet, Set("totalTime", "count", "max"))

    val dp = Datapoint
      .builder()
      .maximum(6.0)
      .minimum(0.0)
      .sum(600.0)
      .sampleCount(1000.0)
      .unit(StandardUnit.SECONDS)
      .timestamp(Instant.now())
      .build()

    definitions.find(_.tags("statistic") == "totalTime").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 10.0)
    }

    definitions.find(_.tags("statistic") == "count").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 6.0)
    }
  }

  test("config for timer-millis no unit") {
    val cfg = ConfigFactory.parseString("""
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "timer-millis"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 3)
    assertEquals(definitions.map(_.tags("statistic")).toSet, Set("totalTime", "count", "max"))

    val dp = Datapoint
      .builder()
      .maximum(6.0)
      .minimum(0.0)
      .sum(600.0)
      .sampleCount(1000.0)
      .unit(StandardUnit.NONE)
      .timestamp(Instant.now())
      .build()

    definitions.find(_.tags("statistic") == "totalTime").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 10.0 / 1000.0)
    }

    definitions.find(_.tags("statistic") == "count").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 6.0 / 1000.0)
    }
  }

  test("config for timer-millis correct unit") {
    val cfg = ConfigFactory.parseString("""
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "timer-millis"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 3)
    assertEquals(definitions.map(_.tags("statistic")).toSet, Set("totalTime", "count", "max"))

    val dp = Datapoint
      .builder()
      .maximum(6.0)
      .minimum(0.0)
      .sum(600.0)
      .sampleCount(1000.0)
      .unit(StandardUnit.MILLISECONDS)
      .timestamp(Instant.now())
      .build()

    definitions.find(_.tags("statistic") == "totalTime").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 10.0 / 1000.0)
    }

    definitions.find(_.tags("statistic") == "count").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 6.0 / 1000.0)
    }
  }

  test("config for dist-summary") {
    val cfg = ConfigFactory.parseString("""
        |name = "Latency"
        |alias = "aws.elb.latency"
        |conversion = "dist-summary"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 3)
    assertEquals(definitions.map(_.tags("statistic")).toSet, Set("totalAmount", "count", "max"))

    val dp = Datapoint
      .builder()
      .maximum(6.0)
      .minimum(0.0)
      .sum(600.0)
      .sampleCount(1000.0)
      .unit(StandardUnit.BYTES)
      .timestamp(Instant.now())
      .build()

    definitions.find(_.tags("statistic") == "totalAmount").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 10.0)
    }

    definitions.find(_.tags("statistic") == "count").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 1000.0 / 60.0)
    }

    definitions.find(_.tags("statistic") == "max").map { d =>
      val m = meta.copy(definition = d)
      assertEquals(m.convert(dp), 6.0)
    }
  }

  test("config for max with unit of millis has correct unit") {
    val cfg = ConfigFactory.parseString("""
        |name = "ReplicaLagMaximum"
        |alias = "aws.aurora.replicaLagMaximum"
        |conversion = "max"
      """.stripMargin)

    val definitions = MetricDefinition.fromConfig(cfg)
    assertEquals(definitions.size, 1)

    val definition = definitions.head
    assertEquals(definition.tags, Map("atlas.dstype" -> "gauge"))

    val dp = Datapoint
      .builder()
      .maximum(15.867)
      .minimum(15.867)
      .sum(15.867)
      .sampleCount(1.0)
      .unit(StandardUnit.MILLISECONDS)
      .timestamp(Instant.now())
      .build()

    val metadata = MetricMetadata(
      MetricCategory("AWS/RDS", 60, -1, Nil, null, Nil, Some(Query.True)),
      definition,
      Nil
    )

    assertEqualsDouble(metadata.convert(dp), 15.867 / 1000.0, 1e-7)
  }
}
