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

import com.netflix.atlas.core.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension

trait Tagger {

  /**
    * Converts a list of cloudwatch dimensions into a tag map that can be used
    * for Atlas.
    */
  def apply(dimensions: List[Dimension]): Map[String, String]

  /**
    * Applies the approved character set to the given data point in preparation for publishing.
    */
  def fixTags(d: Datapoint): Datapoint

}
