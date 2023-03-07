/*
 * Copyright 2014-2023 Netflix, Inc.
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

import com.netflix.atlas.util.XXHasher
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension

/**
  * Container to hold a metric parsed from a Firehose Cloud Watch metric.
  */
case class FirehoseMetric(
  metricStreamName: String,
  namespace: String,
  metricName: String,
  dimensions: List[Dimension],
  datapoint: Datapoint
) {

  def xxHash: Long = {
    var hash = XXHasher.hash(namespace)
    hash = XXHasher.updateHash(hash, metricName)
    dimensions.sortBy(_.name()).foreach { d =>
      hash = XXHasher.updateHash(hash, d.name())
      hash = XXHasher.updateHash(hash, d.value())
    }
    hash
  }
}
