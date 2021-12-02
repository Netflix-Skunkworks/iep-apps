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
package com.netflix.iep.lwc.fwd.admin

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.netflix.iep.lwc.fwd.cw.Report
import munit.FunSuite

import scala.concurrent.duration._

class MarkerServiceExprDetailsSuite extends FunSuite {

  import ExpressionDetails._
  import MarkerServiceImpl._

  private val fwdMetricInfoPurgeLimitMillis = 5.days.toMillis

  private val now = Instant.ofEpochSecond(1551820461L)

  private val metricInfo = FwdMetricInfo("us-east-1", "123", "cpuUsage", Map.empty[String, String])
  private val report = Report(
    now.toEpochMilli,
    ExpressionId(
      "c1",
      ForwardingExpression("name,cpuUsage,:eq", "123", None, "cpuUsage")
    ),
    Some(metricInfo),
    None
  )

  private val scalingPolicy = ScalingPolicy("1", ScalingPolicy.Ec2, "cpuUsage", Nil)
  private val scalingPolicyStatus = ScalingPolicyStatus(false, Some(scalingPolicy))

  private val prevExprDetails = ExpressionDetails(
    report.id,
    now.minusSeconds(10).toEpochMilli,
    report.metricWithTimestamp().toList,
    report.error,
    Map.empty[String, Long],
    List(scalingPolicy)
  )

  test("Make ExpressionDetails") {

    val expected = ExpressionDetails(
      ExpressionId("c1", ForwardingExpression("name,cpuUsage,:eq", "123", None, "cpuUsage")),
      now.toEpochMilli,
      List(
        FwdMetricInfo(
          "us-east-1",
          "123",
          "cpuUsage",
          Map.empty[String, String],
          Some(now.toEpochMilli)
        )
      ),
      None,
      Map.empty[String, Long],
      List(ScalingPolicy("1", ScalingPolicy.Ec2, "cpuUsage", List.empty[MetricDimension]))
    )

    val actual = toExprDetails(
      report,
      scalingPolicyStatus,
      Some(prevExprDetails),
      fwdMetricInfoPurgeLimitMillis,
      now.toEpochMilli
    )

    assertEquals(actual, expected)
  }

  test("NoDataFoundEvent: Report[data=yes]") {
    val actual = toNoDataFoundEvent(report, None)
    assert(actual.isEmpty)
  }

  test("NoDataFoundEvent: Report[data=no] Saved[None]") {
    val expected = Map(NoDataFoundEvent -> report.timestamp)
    val actual = toNoDataFoundEvent(report.copy(metric = None), None)

    assertEquals(actual, expected)
  }

  test("NoDataFoundEvent: Report[data=no] Saved[data=yes]") {
    val expected = Map(NoDataFoundEvent -> report.timestamp)
    val actual = toNoDataFoundEvent(report.copy(metric = None), Some(prevExprDetails))

    assertEquals(actual, expected)
  }

  test("NoDataFoundEvent: Report[data=no] Saved[data=no]") {
    val expected = Map(NoDataFoundEvent -> prevExprDetails.timestamp)
    val actual = toNoDataFoundEvent(
      report.copy(metric = None),
      Some(prevExprDetails.copy(events = Map(NoDataFoundEvent -> prevExprDetails.timestamp)))
    )

    assertEquals(actual, expected)
  }

  test("NoScalingPolicyFoundEvent: Report[sp=unknown]") {
    val actual = toNoScalingPolicyFoundEvent(report, ScalingPolicyStatus(true, None), None)
    assert(actual.isEmpty)
  }

  test("NoScalingPolicyFoundEvent: Report[sp=yes]") {
    val actual = toNoScalingPolicyFoundEvent(report, scalingPolicyStatus, None)
    assert(actual.isEmpty)
  }

  test("NoScalingPolicyFoundEvent: Report[sp=no] Saved[None]") {
    val actual = toNoScalingPolicyFoundEvent(report, ScalingPolicyStatus(false, None), None)
    val expected = Map(NoScalingPolicyFoundEvent -> report.timestamp)

    assertEquals(actual, expected)
  }

  test("NoScalingPolicyFoundEvent: Report[sp=no] Saved[sp=yes]") {
    val actual =
      toNoScalingPolicyFoundEvent(
        report,
        ScalingPolicyStatus(false, None),
        Some(prevExprDetails)
      )
    val expected = Map(NoScalingPolicyFoundEvent -> report.timestamp)

    assertEquals(actual, expected)
  }

  test("NoScalingPolicyFoundEvent: Report[sp=no] Saved[sp=no]") {
    val actual =
      toNoScalingPolicyFoundEvent(
        report,
        ScalingPolicyStatus(false, None),
        Some(
          prevExprDetails.copy(
            events = Map(NoScalingPolicyFoundEvent -> prevExprDetails.timestamp)
          )
        )
      )
    val expected = Map(NoScalingPolicyFoundEvent -> prevExprDetails.timestamp)

    assertEquals(actual, expected)
  }

  test("Make Forwarded Metrics: Report[data=yes] Saved[None]") {
    val expected = List(metricInfo.copy(timestamp = Some(report.timestamp)))
    val actual = toForwardedMetrics(
      report,
      None,
      fwdMetricInfoPurgeLimitMillis,
      now.toEpochMilli
    )

    assertEquals(actual, expected)
  }

  test("Make Forwarded Metrics: Report[data=yes] Saved[data=yes]") {
    val metric1 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "1"),
      timestamp = Some(report.timestamp)
    )
    val metric2 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "2"),
      timestamp = Some(report.timestamp)
    )

    val expected = List(metric1, metric2)
    val actual = toForwardedMetrics(
      report.copy(metric = Some(metric1)),
      Some(prevExprDetails.copy(forwardedMetrics = List(metric1, metric2))),
      fwdMetricInfoPurgeLimitMillis,
      now.toEpochMilli
    )

    assertEquals(actual, expected)
  }

  test("Make Forwarded Metrics: Purge old data") {
    val metric1 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "1"),
      timestamp = Some(now.toEpochMilli)
    )

    val metric2 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "2"),
      timestamp = Some(now.toEpochMilli)
    )

    val metric3 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "3"),
      timestamp = Some(now.minus(6, ChronoUnit.DAYS).toEpochMilli)
    )

    val savedMetrics = List(metric1, metric2, metric3)

    val expected = List(metric1, metric2)
    val actual = toForwardedMetrics(
      report.copy(metric = Some(metric1)),
      Some(prevExprDetails.copy(forwardedMetrics = savedMetrics)),
      fwdMetricInfoPurgeLimitMillis,
      now.toEpochMilli
    )

    assertEquals(actual, expected)
  }

  test("Make Forwarded Metrics: Report[data=no] Saved[None]") {
    val actual = toForwardedMetrics(
      report.copy(metric = None),
      None,
      fwdMetricInfoPurgeLimitMillis,
      now.toEpochMilli
    )

    assert(actual.isEmpty)
  }

  test("Make Scaling Policies: Report[sp=yes] Saved[None]") {
    val expected = List(scalingPolicy)
    val actual = toScalingPolicies(
      Some(scalingPolicy),
      None,
      List(metricInfo)
    )

    assertEquals(actual, expected)
  }

  test("Make Scaling Policies: Report[sp=yes] Saved[sp=yes]") {
    val metric1 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "1"),
      timestamp = Some(now.toEpochMilli)
    )

    val metric2 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "2"),
      timestamp = Some(now.toEpochMilli)
    )

    val policy1 = scalingPolicy.copy(dimensions = List(MetricDimension("nf.asg", "1")))
    val policy2 = scalingPolicy.copy(dimensions = List(MetricDimension("nf.asg", "2")))

    val expected = List(policy1, policy2)
    val actual = toScalingPolicies(
      Some(policy1),
      Some(prevExprDetails.copy(scalingPolicies = List(policy2))),
      List(metric1, metric2)
    )

    assertEquals(actual, expected)
  }

  test("Make Scaling Policies: Purge old data") {
    val metric1 = metricInfo.copy(
      dimensions = Map("nf.asg" -> "1"),
      timestamp = Some(now.toEpochMilli)
    )

    val policy1 = scalingPolicy.copy(dimensions = List(MetricDimension("nf.asg", "1")))
    val policy2 = scalingPolicy.copy(dimensions = List(MetricDimension("nf.asg", "2")))

    val expected = List(policy1)
    val actual = toScalingPolicies(
      Some(policy1),
      Some(prevExprDetails.copy(scalingPolicies = List(policy2))),
      List(metric1)
    )

    assertEquals(actual, expected)
  }
}
