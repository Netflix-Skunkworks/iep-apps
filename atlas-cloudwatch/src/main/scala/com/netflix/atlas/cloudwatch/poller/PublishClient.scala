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
package com.netflix.atlas.cloudwatch.poller

import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasConfig
import com.netflix.spectator.atlas.AtlasRegistry
import com.netflix.spectator.atlas.impl.EvaluatorConfig
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

class PublishClient(val config: PublishConfig) extends StrictLogging {

  private val publishRegistry = new AtlasRegistry(Clock.SYSTEM, config)

  publishRegistry.start()

  logger.info(
    s"registry started for step ${config.step()}, enabled : ${config.lwcEnabled()}, lwc-config URI ${config.configUri}, eval URI ${config.evalUri}"
  )

  def updateGauge(id: Id, value: Double): Unit = {
    publishRegistry.maxGauge(id).set(value)
  }

  def updateCounter(id: Id, value: Double): Unit = {
    publishRegistry.counter(id).add(value)
  }
}

class PublishConfig(
  config: Config,
  publishUri: String,
  configUriParam: String,
  evalUriParam: String,
  status: LeaderStatus,
  registry: Registry
) extends AtlasConfig
    with EvaluatorConfig {

  private val maxMeters = super.maxNumberOfMeters()

  override def get(k: String): String = {
    val prop = s"atlas.cloudwatch.account.routing.$k"
    if (config.hasPath(prop)) config.getString(prop) else null
  }

  override def uri: String = {
    if (publishUri == null)
      "http://localhost:7101/api/v1/publish"
    else
      publishUri
  }

  override def configUri: String = {
    configUriParam
  }

  override def evalUri: String = {
    evalUriParam
  }

  override def enabled(): Boolean = {
    status.hasLeadership
  }

  override def lwcEnabled(): Boolean = {
    enabled()
  }

  override def debugRegistry(): Registry = registry

  /**
   * Value is cached because it is called in a hot-path if there are a lot of new meters
   * for the aggregator.
   */
  override def maxNumberOfMeters(): Int = maxMeters

  override def evaluatorStepSize(): Long = {
    lwcStep().toMillis
  }

  override def parallelMeasurementPolling(): Boolean = {
    true
  }

  override def delayGaugeAggregation(): Boolean = {
    false
  }
}
