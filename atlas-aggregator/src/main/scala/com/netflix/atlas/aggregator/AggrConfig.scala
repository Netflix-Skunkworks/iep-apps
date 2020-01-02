/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas.aggregator

import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasConfig
import com.typesafe.config.Config

class AggrConfig(config: Config, registry: Registry) extends AtlasConfig {

  override def get(k: String): String = {
    val prop = s"netflix.atlas.aggr.registry.$k"
    if (config.hasPath(prop)) config.getString(prop) else null
  }

  override def debugRegistry(): Registry = registry

  override def initialPollingDelay(clock: Clock, stepSize: Long): Long = {
    val now = clock.wallTime()
    val stepBoundary = now / stepSize * stepSize

    // Buffer by 10% of the step interval
    val firstTime = stepBoundary + stepSize / 10
    if (firstTime > now) firstTime - now else firstTime + stepSize - now
  }
}
