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

import java.time.Instant
import com.netflix.atlas.core.model.Query
import junit.framework.TestCase.assertTrue
import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

class MetricDataSuite extends FunSuite {

  private val definition =
    MetricDefinition("test", "alias", Conversions.fromName("sum"), false, Map.empty)

  private val category =
    MetricCategory(
      "namespace",
      60,
      -1,
      List("dimension"),
      null,
      List(definition),
      Some(Query.True)
    )
  private val metadata = MetricMetadata(category, definition, Nil)

  private val monotonicMetadata = metadata.copy(definition = definition.copy(monotonicValue = true))

  private def datapoint(v: Double, c: Double = 1.0): Option[Datapoint] = {
    val d = Datapoint
      .builder()
      .minimum(v)
      .maximum(v)
      .sum(v * c)
      .sampleCount(c)
      .timestamp(Instant.now())
      .unit(StandardUnit.NONE)
      .build()
    Some(d)
  }

  test("access datapoint with no current value") {
    val data = MetricData(metadata, None, None, None)
    assertTrue(data.datapoint.sum.doubleValue().isNaN)
  }

  test("access datapoint with current value") {
    val data = MetricData(metadata, None, datapoint(1.0), None)
    assertEquals(data.datapoint.sum.doubleValue(), 1.0)
  }

  test("access monotonic datapoint with no previous or current value") {
    val data = MetricData(monotonicMetadata, None, None, None)
    assert(data.datapoint.sum.isNaN)
  }

  test("access monotonic datapoint with no current value") {
    val data = MetricData(monotonicMetadata, datapoint(1.0), None, None)
    assert(data.datapoint.sum.isNaN)
  }

  test("access monotonic datapoint with no previous value") {
    val data = MetricData(monotonicMetadata, None, datapoint(1.0), None)
    assert(data.datapoint.sum.isNaN)
  }

  test("access monotonic datapoint, current is larger") {
    val data = MetricData(monotonicMetadata, datapoint(1.0), datapoint(2.0), None)
    assertEquals(data.datapoint.sum.doubleValue(), 1.0)
  }

  test("access monotonic datapoint, previous is larger") {
    val data = MetricData(monotonicMetadata, datapoint(2.0), datapoint(1.0), None)
    assertEquals(data.datapoint.sum.doubleValue(), 0.0)
  }

  test("access monotonic datapoint, previous equals current") {
    val data = MetricData(monotonicMetadata, datapoint(1.0), datapoint(1.0), None)
    assertEquals(data.datapoint.sum.doubleValue(), 0.0)
  }

  test("access monotonic datapoint, current is larger, previous dup") {
    val data = MetricData(monotonicMetadata, datapoint(1.0, 3), datapoint(2.0), None)
    assertEquals(data.datapoint.sum.doubleValue(), 1.0)
  }
}
