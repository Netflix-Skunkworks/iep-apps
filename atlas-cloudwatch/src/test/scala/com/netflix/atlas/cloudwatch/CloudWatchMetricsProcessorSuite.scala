/*
 * Copyright 2014-2024 Netflix, Inc.
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

import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.assertAWSDP
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.assertCWDP
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ce
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.cwv
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.nts
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ts
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.merge
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newValue
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDatapoint
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDimensions
import software.amazon.awssdk.services.cloudwatch.model.Datapoint

import java.time.Instant
import scala.concurrent.duration.DurationInt

class CloudWatchMetricsProcessorSuite extends BaseCloudWatchMetricsProcessorSuite {

  test("normalize") {
    assertEquals(1677706140000L, normalize(1677706164123L, 60))
    assertEquals(1677705900000L, normalize(1677706164123L, 300))
    assertEquals(1677704400000L, normalize(1677706164123L, 3600))
  }

  test("newCacheEntry") {
    assertEquals(cwDP.getNamespace, "AWS/UT1")
    assertEquals(cwDP.getMetric, "SumRate")
    assertEquals(cwDP.getDimensionsCount, 1)
    assertEquals(
      cwDP.getDimensions(0),
      CloudWatchDimension
        .newBuilder()
        .setName("MyTag")
        .setValue("Val")
        .build()
    )
    assertEquals(cwDP.getDataCount, 1)
    assertCWDP(cwDP.getData(0), ts(-2.minutes), Array(39.0, 1.0, 7.0, 19))
    assertEquals(cwDP.getUnit, "Count")
  }

  test("newValue") {
    val nv = newValue(
      Datapoint
        .builder()
        .timestamp(Instant.ofEpochMilli(ts))
        .sum(39.0)
        .minimum(1.0)
        .maximum(7.0)
        .sampleCount(19)
        .build(),
      nts
    )
    assertCWDP(nv, ts, Array(39.0, 1.0, 7.0, 19))
  }

  test("toAWSDatapoint") {
    assertAWSDP(
      toAWSDatapoint(cwDP.getData(0), cwDP.getUnit),
      Array(39.0, 1.0, 7.0, 19),
      ts(-2.minute),
      "Count"
    )
  }

  test("toAWSDimensions") {
    val dimensions = toAWSDimensions(cwDP)
    assertEquals(dimensions.size, cwDP.getDimensionsCount)
    assertEquals(dimensions(0).name(), cwDP.getDimensions(0).getName)
    assertEquals(dimensions(0).value(), cwDP.getDimensions(0).getValue)
  }

  test("merge - new and published") {
    val a = ce(
      List(
        cwv(-9.minutes, -8.minutes, true)
      )
    )
    val b = ce(
      List(
        cwv(-9.minutes, -8.minutes, false),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    val expected = ce(
      List(
        cwv(-9.minutes, -8.minutes, true),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    assertEquals(merge(a, b), expected)
    assertEquals(merge(b, a), expected)
  }

  test("merge - two values") {
    val a = ce(
      List(
        cwv(-9.minutes, -8.minutes, false)
      )
    )
    val b = ce(
      List(
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    val expected = ce(
      List(
        cwv(-9.minutes, -8.minutes, false),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    assertEquals(merge(a, b), expected)
    assertEquals(merge(b, a), expected)
  }

  test("merge - insert between values") {
    val a = ce(
      List(
        cwv(-9.minutes, -8.minutes, false)
      )
    )
    val b = ce(
      List(
        cwv(-10.minutes, -9.minutes, false),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    val expected = ce(
      List(
        cwv(-10.minutes, -9.minutes, false),
        cwv(-9.minutes, -8.minutes, false),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    assertEquals(merge(a, b), expected)
    assertEquals(merge(b, a), expected)
  }

  test("merge - empty") {
    val a = ce(List.empty)
    val b = ce(
      List(
        cwv(-9.minutes, -8.minutes, false),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    val expected = ce(
      List(
        cwv(-9.minutes, -8.minutes, false),
        cwv(-8.minutes, -7.minutes, false)
      )
    )
    assertEquals(merge(a, b), expected)
    assertEquals(merge(b, a), expected)
  }

  //  test("print metrics") {
  //    rules.rules.foreachEntry { (outerKey, nested) =>
  //      nested.foreachEntry { (innerKey, catTuple) =>
  //        val (category, metrics) = catTuple
  //        val metric = metrics.head
  //        println(s"${category.namespace}, ${metric.name}, ${metric.alias}, ${category.dimensions.mkString("; ")}")
  //
  //      }
  //    }
  //  }

}
