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
import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import java.time.Instant

class ConversionsSuite extends FunSuite {

  private val dp = Datapoint
    .builder()
    .minimum(1.0)
    .maximum(5.0)
    .sum(6.0)
    .sampleCount(2.0)
    .timestamp(Instant.now())
    .unit(StandardUnit.NONE)
    .build()

  private def newDatapoint(v: Double, unit: StandardUnit = StandardUnit.NONE): Datapoint = {
    Datapoint
      .builder()
      .minimum(v)
      .maximum(v)
      .sum(v)
      .sampleCount(1.0)
      .timestamp(Instant.now())
      .unit(unit)
      .build()
  }

  test("min") {
    val cnv = Conversions.fromName("min")
    val v = cnv(null, dp)
    assertEquals(v, 1.0)
  }

  test("max") {
    val cnv = Conversions.fromName("max")
    val v = cnv(null, dp)
    assertEquals(v, 5.0)
  }

  test("sum") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, dp)
    assertEquals(v, 6.0)
  }

  test("count") {
    val cnv = Conversions.fromName("count")
    val v = cnv(null, dp)
    assertEquals(v, 2.0)
  }

  test("avg") {
    val cnv = Conversions.fromName("avg")
    val v = cnv(null, dp)
    assertEquals(v, 3.0)
  }

  test("rate") {
    val cnv = Conversions.fromName("sum,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 300, -1, Nil, null, Nil, Some(Query.True)),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, dp)
    assertEquals(v, 6.0 / 300.0)
  }

  test("rate already") {
    val cnv = Conversions.fromName("sum,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 300, -1, Nil, null, Nil, Some(Query.True)),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, newDatapoint(6.0, StandardUnit.BYTES_SECOND))
    assertEquals(v, 6.0)
  }

  test("sum rate already, no unit, 5s") {
    val cnv = Conversions.fromName("sum,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 5, -1, Nil, null, Nil, Some(Query.True)),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, newDatapoint(280.0, StandardUnit.NONE))
    // still converts since we don't have units.
    assertEquals(v, 56.0)
  }

  test("sum rate already, bits/sec, 5s") {
    val cnv = Conversions.fromName("sum,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 5, -1, Nil, null, Nil, Some(Query.True)),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, newDatapoint(280.0, StandardUnit.BITS_SECOND))
    // bits per second
    assertEquals(v, 35.0)
  }

  test("max rate already, no unit, 5s") {
    val cnv = Conversions.fromName("max,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 5, -1, Nil, null, Nil, Some(Query.True)),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, newDatapoint(56.0, StandardUnit.NONE))
    assertEquals(v, 11.2)
  }

  test("max rate already, bits/sec, 5s") {
    val cnv = Conversions.fromName("max,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 5, -1, Nil, null, Nil, Some(Query.True)),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, newDatapoint(56.0, StandardUnit.BITS_SECOND))
    assertEquals(v, 7.0)
  }

  test("bad conversion") {
    intercept[IllegalArgumentException] {
      Conversions.fromName("foo")
    }
  }

  test("empty conversion") {
    intercept[IllegalArgumentException] {
      Conversions.fromName("")
    }
  }

  test("missing conversion") {
    intercept[IllegalArgumentException] {
      Conversions.fromName("rate") // Rate must be used with another conversion
    }
  }

  test("unit count") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.COUNT))
    assertEquals(v, 42.0)
  }

  test("unit bits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.BITS))
    assertEquals(v, 42.0 / 8.0)
  }

  test("unit kilobits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.KILOBITS))
    assertEquals(v, 1e3 * 42.0 / 8.0)
  }

  test("unit megabits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MEGABITS))
    assertEquals(v, 1e6 * 42.0 / 8.0)
  }

  test("unit gigabits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.GIGABITS))
    assertEquals(v, 1e9 * 42.0 / 8.0)
  }

  test("unit terabits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.TERABITS))
    assertEquals(v, 1e12 * 42.0 / 8.0)
  }

  test("unit bytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.BYTES))
    assertEquals(v, 42.0)
  }

  test("unit kilobytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.KILOBYTES))
    assertEquals(v, 1e3 * 42.0)
  }

  test("unit megabytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MEGABYTES))
    assertEquals(v, 1e6 * 42.0)
  }

  test("unit gigabytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.GIGABYTES))
    assertEquals(v, 1e9 * 42.0)
  }

  test("unit terabytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.TERABYTES))
    assertEquals(v, 1e12 * 42.0)
  }

  test("unit bits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.BITS_SECOND))
    assertEquals(v, 42.0 / 8.0)
  }

  test("unit kilobits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.KILOBITS_SECOND))
    assertEquals(v, 1e3 * 42.0 / 8.0)
  }

  test("unit megabits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MEGABITS_SECOND))
    assertEquals(v, 1e6 * 42.0 / 8.0)
  }

  test("unit gigabits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.GIGABITS_SECOND))
    assertEquals(v, 1e9 * 42.0 / 8.0)
  }

  test("unit terabits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.TERABITS_SECOND))
    assertEquals(v, 1e12 * 42.0 / 8.0)
  }

  test("unit bytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.BYTES_SECOND))
    assertEquals(v, 42.0)
  }

  test("unit kilobytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.KILOBYTES_SECOND))
    assertEquals(v, 1e3 * 42.0)
  }

  test("unit megabytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MEGABYTES_SECOND))
    assertEquals(v, 1e6 * 42.0)
  }

  test("unit gigabytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.GIGABYTES_SECOND))
    assertEquals(v, 1e9 * 42.0)
  }

  test("unit terabytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.TERABYTES_SECOND))
    assertEquals(v, 1e12 * 42.0)
  }

  test("unit microseconds") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MICROSECONDS))
    assertEquals(v, 1e-6 * 42.0)
  }

  test("unit milliseconds") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MILLISECONDS))
    assertEquals(v, 1e-3 * 42.0)
  }

  test("unit seconds") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.SECONDS))
    assertEquals(v, 42.0)
  }

  test("unit percent") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.PERCENT))
    assertEquals(v, 42.0)
  }

  test("ratio to percent") {
    val cnv = Conversions.fromName("sum,percent")
    val v = cnv(null, newDatapoint(0.42, StandardUnit.PERCENT))
    assertEquals(v, 42.0)
  }

  test("unit none") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.NONE))
    assertEquals(v, 42.0)
  }

  test("multiply") {
    val cnv = Conversions.multiply(Conversions.fromName("sum"), 100.0)
    val v = cnv(null, newDatapoint(42.0))
    assertEquals(v, 4200.0)
  }

  test("dstype for max") {
    assertEquals(Conversions.determineDsType("max"), "gauge")
  }

  test("dstype for sum") {
    assertEquals(Conversions.determineDsType("sum"), "gauge")
  }

  test("dstype for count") {
    assertEquals(Conversions.determineDsType("count"), "gauge")
  }

  test("dstype for sum,rate") {
    assertEquals(Conversions.determineDsType("sum,rate"), "rate")
  }

  test("dstype for count,rate") {
    assertEquals(Conversions.determineDsType("count,rate"), "rate")
  }
}
