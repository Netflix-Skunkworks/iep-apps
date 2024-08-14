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
package com.netflix.atlas.aggregator

import java.security.SecureRandom
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasConfig
import com.netflix.spectator.atlas.Publisher
import com.netflix.spectator.atlas.impl.EvaluatorConfig
import com.netflix.spectator.atlas.impl.QueryIndex
import com.netflix.spectator.impl.Cache
import com.typesafe.config.Config

class AggrConfig(
  val config: Config,
  registry: Registry,
  client: PekkoClient
) extends AtlasConfig
    with EvaluatorConfig {

  private val maxMeters = super.maxNumberOfMeters()

  override def get(k: String): String = {
    val prop = s"netflix.atlas.aggr.registry.$k"
    if (config.hasPath(prop)) config.getString(prop) else null
  }

  override def debugRegistry(): Registry = registry

  override def initialPollingDelay(clock: Clock, stepSize: Long): Long = {
    val now = clock.wallTime()
    val stepBoundary = now / stepSize * stepSize

    // Random delay to spread out load. Default implementation from the super class assumes
    // relatively random start time across instances so uses now. Here we use a random number
    // since there are multiple registries per aggregator with the same start time.
    val random = new SecureRandom()

    // To give it plenty of time, we give a 5% buffer after step boundary and 20% before
    // the next step boundary.
    val offset = stepSize / 20
    val range = stepSize - 5 * offset
    val delay = (range * random.nextDouble()).toLong + offset

    // Check if the current delay is after the current time
    val firstTime = stepBoundary + delay
    if (firstTime > now) firstTime - now else firstTime + stepSize - now
  }

  /**
    * Value is cached because it is called in a hot-path if there are a lot of new meters
    * for the aggregator.
    */
  override def maxNumberOfMeters(): Int = maxMeters

  /**
    * Set to null since this will get corrected before it gets to the registry.
    */
  override def validTagCharacters(): String = null

  override def publisher(): Publisher = {
    new PekkoPublisher(this, client)
  }

  override def evaluatorStepSize(): Long = {
    lwcStep().toMillis
  }

  override def parallelMeasurementPolling(): Boolean = {
    true
  }

  override def delayGaugeAggregation(): Boolean = {
    config.getBoolean("atlas.aggregator.delay-gauge-aggregation")
  }

  override def indexCacheSupplier[T](): QueryIndex.CacheSupplier[T] = { () =>
    new CaffeineCache[T].asInstanceOf[Cache[String, java.util.List[QueryIndex[T]]]]
  }
}
