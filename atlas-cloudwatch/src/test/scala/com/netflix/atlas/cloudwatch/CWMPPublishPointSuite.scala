/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ce
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.cwv
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.nts

import scala.concurrent.duration.DurationInt

class CWMPPublishPointSuite extends BaseCloudWatchMetricsProcessorSuite {

  test("getPublishPoint - empty") {
    // shouldn't happen
    val cache = ce(List.empty)
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics()
  }

  test("getPublishPoint - 1 unpublished") {
    val cache = ce(
      List(cwv(-2.minutes, -1.minutes, false))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache)
    assertMetrics(
      current = 1.minutes.toMillis,
      wall = 2.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 1 unpublished, within grace period") {
    val cache = ce(
      List(cwv(-3.minutes, -2.minutes, false))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache)
    assertMetrics(
      grace = 2.minutes.toMillis,
      wall = 3.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 1 unpublished, out of grace period") {
    val cache = ce(
      List(cwv(-5.minutes, -4.minutes, false))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics(unpublished = 4.minutes.toMillis)
  }

  test("getPublishPoint - 1 unpublished, grace override") {
    val cache = ce(
      List(cwv(-5.minutes, -4.minutes, false))
    )
    val category = MetricCategory("AWS/DynamoDB", 60, 4, List("MyTag"), null, List.empty, null)
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache, true)
    assertMetrics(
      grace = 4.minutes.toMillis,
      wall = 5.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 1 expired") {
    val cache = ce(
      List(cwv(-6.minutes, -5.minutes, false))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics()
  }

  test("getPublishPoint - 2 values, newest unpublished") {
    val cache = ce(
      List(
        cwv(-3.minutes, -2.minutes, true),
        cwv(-2.minutes, -1.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 1, cache)
    assertMetrics(
      current = 1.minutes.toMillis,
      wall = 2.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 2 values, newest unpublished, within grace period") {
    val cache = ce(
      List(
        cwv(-4.minutes, -3.minutes, true),
        cwv(-3.minutes, -2.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 1, cache)
    assertMetrics(
      grace = 2.minutes.toMillis,
      wall = 3.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 2 values, newest unpublished, out of grace period") {
    val cache = ce(
      List(
        cwv(-6.minutes, -5.minutes, true),
        cwv(-5.minutes, -4.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics(unpublished = 4.minutes.toMillis)
  }

  test("getPublishPoint - 2 values, newest expired") {
    val cache = ce(
      List(
        cwv(-7.minutes, -6.minutes, true),
        cwv(-6.minutes, -5.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics()
  }

  test("getPublishPoint - 2 unpublished, ordered") {
    val cache = ce(
      List(
        cwv(-2.minutes, -1.minutes, false),
        cwv(-1.minutes, -0.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache)
    assertMetrics(
      current = 1.minutes.toMillis,
      wall = 2.minutes.plus(55.seconds).toMillis,
      pubIndex = 1
    )
  }

  test("getPublishPoint - 2 unpublished, out of order") {
    val cache = ce(
      List(
        cwv(-2.minutes, -0.minutes, false),
        cwv(-1.minutes, -1.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache)
    assertMetrics(
      current = 0,
      wall = 2.minutes.plus(55.seconds).toMillis,
      pubIndex = 1
    )
  }

  test("getPublishPoint - 2, newest already published, within cutoff") {
    val cache = ce(
      List(
        cwv(-2.minutes, -1.minutes, false),
        cwv(-1.minutes, -1.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics(alreadyPublished = 1.minutes.toMillis)
  }

  test("getPublishPoint - 2, newest already published") {
    val cache = ce(
      List(
        cwv(-3.minutes, -2.minutes, false),
        cwv(-2.minutes, -2.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics()
  }

  test("getPublishPoint - 2, oldest unpublished, out of order") {
    val cache = ce(
      List(
        cwv(-2.minutes, -0.minutes, false),
        cwv(-1.minutes, -1.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics(alreadyPublished = 1.minutes.toMillis)
  }

  test("getPublishPoint - 2 all published") {
    val cache = ce(
      List(
        cwv(-3.minutes, -2.minutes, true),
        cwv(-2.minutes, -1.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics(alreadyPublished = 1.minutes.toMillis)
  }

  test("getPublishPoint - 2 all published and expired") {
    val cache = ce(
      List(
        cwv(-4.minutes, -3.minutes, true),
        cwv(-3.minutes, -2.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics()
  }

  test("getPublishPoint - 3 values, newest unpublished, ordered") {
    val cache = ce(
      List(
        cwv(-4.minutes, -3.minutes, true),
        cwv(-3.minutes, -2.minutes, true),
        cwv(-2.minutes, -1.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 2, cache)
    assertMetrics(
      current = 1.minutes.toMillis,
      wall = 2.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 3 unpublished, oldest within grace") {
    val cache = ce(
      List(
        cwv(-3.minutes, -2.minutes, false),
        cwv(-2.minutes, -1.minutes, false),
        cwv(-1.minutes, -0.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache)
    assertMetrics(
      grace = 2.minutes.toMillis,
      wall = 3.minutes.plus(55.seconds).toMillis,
      pubIndex = 2
    )
  }

  test("getPublishPoint - 3 unpublished, out of order, oldest within grace") {
    val cache = ce(
      List(
        cwv(-3.minutes, -1.minutes, false),
        cwv(-2.minutes, -1.minutes, false),
        cwv(-1.minutes, -2.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 0, cache)
    assertMetrics(
      current = 1.minutes.toMillis,
      wall = 3.minutes.plus(55.seconds).toMillis,
      pubIndex = 2
    )
  }

  test("getPublishPoint - 3, middle published, out of order, oldest within grace") {
    val cache = ce(
      List(
        cwv(-3.minutes, -1.minutes, false),
        cwv(-2.minutes, -1.minutes, true),
        cwv(-1.minutes, -2.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 2, cache)
    assertMetrics(
      grace = 2.minutes.toMillis,
      wall = 1.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - 3 unpublished, oldest out of grace") {
    val cache = ce(
      List(
        cwv(-5.minutes, -4.minutes, false),
        cwv(-4.minutes, -3.minutes, false),
        cwv(-3.minutes, -2.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), 1, cache)
    assertMetrics(
      grace = 3.minutes.toMillis,
      unpublished = 4.minutes.toMillis,
      wall = 4.minutes.plus(55.seconds).toMillis,
      pubIndex = 1
    )
  }

  test("getPublishPoint - 3 unpublished, middle unpublished") {
    val cache = ce(
      List(
        cwv(-4.minutes, -3.minutes, true),
        cwv(-3.minutes, -2.minutes, false),
        cwv(-2.minutes, -1.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category), -1, cache, false)
    assertMetrics(
      alreadyPublished = 1.minutes.toMillis
    )
  }

  test("getPublishPoint - 5m: 1 published, republished") {
    val cache = ce(
      List(cwv(-6.minutes, -5.minutes, true))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 0, cache, false)
    assertMetrics(
      republished = 5.minutes.toMillis,
      wall = 6.minutes.plus(55.seconds).toMillis,
      pubIndex = 0,
      fiveMin = true
    )
  }

  test("getPublishPoint - 5m: 1 published, in grace period") {
    val cache = ce(
      List(cwv(-7.minutes, -6.minutes, true))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 0, cache, false)
    assertMetrics(
      republished = 6.minutes.toMillis,
      wall = 7.minutes.plus(55.seconds).toMillis,
      pubIndex = 0,
      fiveMin = true
    )
  }

  test("getPublishPoint - 5m: 1 published, expired") {
    val cache = ce(
      List(cwv(-9.minutes, -8.minutes, true))
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), -1, cache, false)
    assertMetrics()
  }

  test("getPublishPoint - 5m: 2, oldest grace") {
    val cache = ce(
      List(
        cwv(-10.minutes, -9.minutes, false),
        cwv(-5.minutes, -4.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 0, cache)
    assertMetrics(
      grace = 9.minutes.toMillis,
      wall = 10.minutes.plus(55.seconds).toMillis,
      pubIndex = 1,
      fiveMin = true
    )
  }

  test("getPublishPoint - 5m: 2, oldest published") {
    val cache = ce(
      List(
        cwv(-10.minutes, -9.minutes, true),
        cwv(-5.minutes, -4.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 1, cache)
    assertMetrics(
      current = 4.minutes.toMillis,
      wall = 5.minutes.plus(55.seconds).toMillis,
      pubIndex = 0,
      fiveMin = true
    )
  }

  test("getPublishPoint - 5m: 2, republished") {
    val cache = ce(
      List(
        cwv(-10.minutes, -9.minutes, true),
        cwv(-5.minutes, -4.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 1, cache, false)
    assertMetrics(
      republished = 4.minutes.toMillis,
      wall = 5.minutes.plus(55.seconds).toMillis,
      pubIndex = 0,
      fiveMin = true
    )
  }

  test("getPublishPoint - 5m: 2, oldest unpublished, republish of latest") {
    val cache = ce(
      List(
        cwv(-10.minutes, -9.minutes, false),
        cwv(-5.minutes, -4.minutes, true)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 1, cache, false)
    assertMetrics(
      republished = 4.minutes.toMillis,
      wall = 5.minutes.plus(55.seconds).toMillis,
      pubIndex = 0,
      fiveMin = true
    )
  }

  test("getPublishPoint - 5m: 2, oldest unpublished") {
    val cache = ce(
      List(
        cwv(-20.minutes, -19.minutes, false),
        cwv(-15.minutes, -14.minutes, false)
      )
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, category5m), 1, cache)
    assertMetrics(
      unpublished = 19.minutes.toMillis,
      grace = 14.minutes.toMillis,
      wall = 15.minutes.plus(55.seconds).toMillis,
      pubIndex = 0,
      fiveMin = true
    )
  }

  test("getPublishPoint - need two, one value") {
    val cache = ce(
      List(cwv(-2.minutes, -1.minutes, false))
    )
    val mono = MetricCategory(
      "AWS/DynamoDB",
      60,
      -1,
      List("MyTag"),
      null,
      List(MetricDefinition("SumRate", "sum.rate", null, true, Map.empty, null)),
      null
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, mono), -1, cache, false)
    assertMetrics(
      needTwo = 1
    )
  }

  test("getPublishPoint - need two, one value too old") {
    val cache = ce(
      List(cwv(-6.minutes, -5.minutes, false))
    )
    val mono = MetricCategory(
      "AWS/DynamoDB",
      60,
      -1,
      List("MyTag"),
      null,
      List(MetricDefinition("SumRate", "sum.rate", null, true, Map.empty, null)),
      null
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, mono), -1, cache, false)
    assertMetrics(
      needTwo = 1
    )
  }

  test("getPublishPoint - need two, one of two values too old") {
    val cache = ce(
      List(cwv(-6.minutes, -5.minutes, false), cwv(-2.minutes, -1.minutes, false))
    )
    val mono = MetricCategory(
      "AWS/DynamoDB",
      60,
      -1,
      List("MyTag"),
      null,
      List(MetricDefinition("SumRate", "sum.rate", null, true, Map.empty, null)),
      null
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, mono), -1, cache, false)
    assertMetrics(
      needTwo = 1
    )
  }

  test("getPublishPoint - need two") {
    val cache = ce(
      List(cwv(-3.minutes, -2.minutes, false), cwv(-2.minutes, -1.minutes, false))
    )
    val mono = MetricCategory(
      "AWS/DynamoDB",
      60,
      -1,
      List("MyTag"),
      null,
      List(MetricDefinition("SumRate", "sum.rate", null, true, Map.empty, null)),
      null
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, mono), 1, cache)
    assertMetrics(
      current = 1.minutes.toMillis,
      wall = 2.minutes.plus(55.seconds).toMillis,
      pubIndex = 0
    )
  }

  test("getPublishPoint - need two, large gap") {
    val cache = ce(
      List(cwv(-7.minutes, -6.minutes, false), cwv(-2.minutes, -1.minutes, false))
    )
    val mono = MetricCategory(
      "AWS/DynamoDB",
      60,
      -1,
      List("MyTag"),
      null,
      List(MetricDefinition("SumRate", "sum.rate", null, true, Map.empty, null)),
      null
    )
    assertPublishPoint(processor.getPublishPoint(cache, nts, mono), -1, cache, false)
    assertMetrics(needTwo = 1)
  }

  def assertPublishPoint(
    tuple: (Int, CloudWatchCacheEntry),
    index: Int,
    original: CloudWatchCacheEntry,
    updated: Boolean = true
  ): Unit = {
    assertEquals(tuple._1, index)
    if (updated) {
      assertNotEquals(original, tuple._2)
    } else {
      assertEquals(original, tuple._2)
    }
  }

  def assertMetrics(
    current: Long = 0,
    grace: Long = 0,
    republished: Long = 0,
    unpublished: Long = 0,
    alreadyPublished: Long = 0,
    wall: Long = 0,
    pubIndex: Long = 0,
    needTwo: Long = 0,
    fiveMin: Boolean = false
  ): Unit = {
    assertEquals(
      registry
        .distributionSummary(
          processor.published
            .withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate", "state", "current")
        )
        .totalAmount(),
      current
    )
    assertEquals(
      registry
        .distributionSummary(
          processor.published
            .withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate", "state", "grace")
        )
        .totalAmount(),
      grace
    )
    assertEquals(
      registry
        .distributionSummary(
          processor.published
            .withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate", "state", "republished")
        )
        .totalAmount(),
      republished
    )
    assertEquals(
      registry
        .distributionSummary(
          processor.unpublished.withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate")
        )
        .totalAmount(),
      unpublished
    )
    assertEquals(
      registry
        .distributionSummary(
          processor.invalid.withTags(
            "aws.namespace",
            "AWS/UT1",
            "aws.metric",
            "SumRate",
            "reason",
            "alreadyPublished"
          )
        )
        .totalAmount(),
      alreadyPublished
    )
    val period = if (fiveMin) "300" else "60"
    assertEquals(
      registry
        .distributionSummary(
          processor.wallOffset
            .withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate", "period", period)
        )
        .totalAmount(),
      wall
    )
    assertEquals(
      registry
        .distributionSummary(
          processor.indexOffset
            .withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate", "period", period)
        )
        .totalAmount(),
      pubIndex
    )
    assertEquals(
      registry
        .counter(
          processor.invalid.withTags(
            "aws.namespace",
            "AWS/UT1",
            "aws.metric",
            "SumRate",
            "reason",
            "needTwoValues"
          )
        )
        .count(),
      needTwo
    )
  }
}
