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

import com.netflix.atlas.cloudwatch.MetricData.DatapointNaN
import com.netflix.atlas.cloudwatch.MetricData.Zero
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
      current.getOrElse {
        // We send 0 values for gaps in CloudWatch data because previously, users were
        // confused or concerned when they saw spans of NaN values in the data reported.
        // Those spans occur especially for low-volume resources and resources where the
        // only available period is greater than than the period configured for the
        // `MetricCategory` (although, that may indicate a misconfiguration).
        //
        // This implementation reports `0` if there's no configured timeout or if we've
        // received at least one datapoint until the timeout is exceeded. It reports `NaN`
        // until the first datapoint is received or for no data within and beyond the
        // timeout threshold.
        //
        // Requiring at least one datapoint prevents interpolating `0` from startup until
        // the timeout for obsolete resources.  It may result in NaN gaps for low volume
        // resources when deploying. But that is likely preferable to suddenly and briefly
        // reporting `0` for obsolete resources and possibly triggering alerts for those
        // with expressions that use wildcards for the resource selector.
        val reportNaN = meta.category.timeout.exists { timeout =>
          lastReportedTimestamp.fold(true) { timestamp =>
            java.time.Duration.between(timestamp, now).compareTo(timeout) > 0
          }
        }

        if (reportNaN) DatapointNaN else Zero
      }
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

  private val Zero = Datapoint
    .builder()
    .minimum(0.0)
    .maximum(0.0)
    .sum(0.0)
    .sampleCount(0.0)
    .timestamp(Instant.now())
    .unit(StandardUnit.NONE)
    .build()
}
