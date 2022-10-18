/*
 * Copyright 2014-2022 Netflix, Inc.
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

import akka.actor.ActorSystem
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasRegistry
import com.typesafe.config.Config

import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class AtlasAggregatorService @Inject() (
  config: Config,
  clock: Clock,
  registry: Registry,
  system: ActorSystem
) extends AbstractService
    with Aggregator {

  private val n = math.max(1, Runtime.getRuntime.availableProcessors() / 2)
  private val aggrCfg = new AggrConfig(config, registry, system)

  private val registries = (0 until n)
    .map(_ => new AtlasRegistry(clock, aggrCfg))
    .toArray

  override def startImpl(): Unit = {
    registries.foreach(_.start())
  }

  override def stopImpl(): Unit = {
    registries.foreach(_.stop())
  }

  def lookup(id: Id): AtlasRegistry = {
    // Max is needed because for Integer.MIN_VALUE the abs value will be negative
    val i = math.max(math.abs(id.hashCode()), 0) % n
    registries(i)
  }

  override def add(id: Id, value: Double): Unit = {
    lookup(id).counter(id).add(value)
  }

  override def max(id: Id, value: Double): Unit = {
    lookup(id).maxGauge(id).set(value)
  }
}
