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
package com.netflix.iep.lwc.fwd.cw

case class Report(
  timestamp: Long,
  id: ExpressionId,
  metric: Option[FwdMetricInfo],
  error: Option[Throwable]
) {

  def metricWithTimestamp(): Option[FwdMetricInfo] = {
    metric.map(_.copy(timestamp = Some(timestamp)))
  }
}

case class FwdMetricInfo(
  region: String,
  account: String,
  name: String,
  dimensions: Map[String, String],
  timestamp: Option[Long] = None
) {

  def equalsIgnoreTimestamp(that: FwdMetricInfo): Boolean = {
    this.copy(timestamp = None) == that.copy(timestamp = None)
  }

}
