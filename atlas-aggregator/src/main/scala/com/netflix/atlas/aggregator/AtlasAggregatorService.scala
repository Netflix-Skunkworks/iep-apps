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

import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasRegistry
import com.typesafe.config.Config

class AtlasAggregatorService(
  config: Config,
  clock: Clock,
  registry: Registry,
  client: PekkoClient
) extends AbstractService
    with Aggregator {

  private val aggrCfg = new AggrConfig(config, registry, client)

  private val aggrRegistry = new AtlasRegistry(clock, aggrCfg)

  def atlasRegistry: Registry = aggrRegistry

  override def startImpl(): Unit = {
    aggrRegistry.start()
  }

  override def stopImpl(): Unit = {
    aggrRegistry.stop()
  }

  def lookup(id: Id): AtlasRegistry = {
    aggrRegistry
  }

  override def add(id: Id, value: Double): Unit = {
    aggrRegistry.counter(id).add(value)
  }

  override def max(id: Id, value: Double): Unit = {
    aggrRegistry.maxGauge(id).set(value)
  }
}
