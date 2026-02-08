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

import org.apache.pekko.util.ccompat.JavaConverters.SeqHasAsJava
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest
import software.amazon.awssdk.services.cloudwatch.model.Metric
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery
import software.amazon.awssdk.services.cloudwatch.model.MetricStat
import software.amazon.awssdk.services.cloudwatch.model.Statistic

import java.time.Instant

/**
  * Metadata for a particular metric to retrieve from CloudWatch.
  */
case class MetricMetadata(
  category: MetricCategory,
  definition: MetricDefinition,
  dimensions: List[Dimension]
) {

  def convert(d: Datapoint): Double = definition.conversion(this, d)

  /** Legacy GetMetricStatistics request (used by non-batch path / tests). */
  def toGetRequest(s: Instant, e: Instant): GetMetricStatisticsRequest =
    GetMetricStatisticsRequest
      .builder()
      .metricName(definition.name)
      .namespace(category.namespace)
      .dimensions(dimensions.asJava)
      .statistics(Statistic.MAXIMUM, Statistic.MINIMUM, Statistic.SUM, Statistic.SAMPLE_COUNT)
      .period(category.period)
      .startTime(s)
      .endTime(e)
      .build()
}

/**
 * Helpers for building GetMetricData queries from MetricMetadata.
 */
object MetricMetadata {

  private val AllStats: List[(String, String)] = List(
    "max" -> "Maximum",
    "min" -> "Minimum",
    "sum" -> "Sum",
    "cnt" -> "SampleCount"
  )

  /**
   * Create 4 MetricDataQuery entries for this metric (Max, Min, Sum, SampleCount).
   *
   * @param meta   metric metadata (namespace, name, dims, period)
   * @param baseId base id for this metric's queries, e.g. "m0" → "m0_max", "m0_min", ...
   */
  def toMetricDataQueries(meta: MetricMetadata, baseId: String): List[MetricDataQuery] = {
    val m: Metric =
      Metric
        .builder()
        .namespace(meta.category.namespace)
        .metricName(meta.definition.name)
        .dimensions(meta.dimensions.asJava)
        .build()

    def query(idSuffix: String, stat: String): MetricDataQuery =
      MetricDataQuery
        .builder()
        .id(s"${baseId}_$idSuffix")
        .metricStat(
          MetricStat
            .builder()
            .metric(m)
            .period(meta.category.period)
            .stat(stat)
            .build()
        )
        .returnData(true)
        .build()

    AllStats.map { case (suffix, statName) => query(suffix, statName) }
  }
}
