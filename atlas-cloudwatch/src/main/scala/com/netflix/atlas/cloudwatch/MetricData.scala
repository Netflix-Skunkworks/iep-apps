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

import com.netflix.atlas.cloudwatch.MetricData.DatapointNaN
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import java.time.Instant

case class MetricData(
  meta: MetricMetadata,
  previous: Option[Datapoint],
  current: Option[Datapoint],
  lastReportedTimestamp: Option[Instant]
) {

  def datapoint(now: Instant = Instant.now): Datapoint = {
    if (meta.definition.monotonicValue) {
      previous.fold(DatapointNaN) { p =>
        // For a monotonic counter, use the max statistic. These will typically have a
        // single reporting source that maintains the state over time. If the sample count
        // is larger than one, it will be a spike due to the reporter sending the value
        // multiple times within that interval. The max will allow us to ignore those
        // spikes and get the last written value.
        val c = current.getOrElse(DatapointNaN)
        val delta = math.max(c.maximum - p.maximum, 0.0)
        Datapoint
          .builder()
          .minimum(delta)
          .maximum(delta)
          .sum(delta)
          .sampleCount(c.sampleCount)
          .timestamp(c.timestamp)
          .unit(c.unit)
          .build()
      }
    } else {
      // now reporting NaN to better align with CloudWatch were values will be missing.
      // Timeouts were rarely used.
      current.getOrElse(DatapointNaN)
    }
  }
}

object MetricData {

  private val DatapointNaN = Datapoint
    .builder()
    .minimum(Double.NaN)
    .maximum(Double.NaN)
    .sum(Double.NaN)
    .sampleCount(Double.NaN)
    .timestamp(Instant.now())
    .unit(StandardUnit.NONE)
    .build()

}
