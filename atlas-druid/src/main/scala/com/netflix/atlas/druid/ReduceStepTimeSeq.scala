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
package com.netflix.atlas.druid

import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeq

class ReduceStepTimeSeq(ts: TimeSeq, val step: Long) extends TimeSeq {

  require(
    ts.step % step == 0,
    "original step must be a multiple of reduced step"
  )

  def dsType: DsType = ts.dsType

  def apply(timestamp: Long): Double = {
    val t = timestamp / ts.step * ts.step
    ts(t + ts.step)
  }
}
