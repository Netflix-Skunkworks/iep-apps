/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.iep.admin.EndpointMapping
import com.netflix.iep.admin.endpoints.SpectatorEndpoint
import com.netflix.iep.config.ConfigManager
import com.netflix.iep.config.DynamicConfigManager
import org.apache.pekko.actor.ActorSystem
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

import java.util.Optional

@Configuration
class AppConfiguration {

  @Bean
  def pekkoClient(
    registry: Optional[Registry],
    system: ActorSystem
  ): PekkoClient = {
    val r = registry.orElseGet(() => new NoopRegistry)
    new PekkoClient(r, system)
  }

  @Bean
  def atlasAggregatorService(
    config: Optional[Config],
    registry: Optional[Registry],
    client: PekkoClient
  ): AtlasAggregatorService = {
    val c = config.orElseGet(() => ConfigFactory.load())
    val r = registry.orElseGet(() => new NoopRegistry)
    new AtlasAggregatorService(c, Clock.SYSTEM, r, client)
  }

  @Bean
  def shardedAggregatorService(
    configManager: Optional[DynamicConfigManager],
    registry: Optional[Registry],
    client: PekkoClient,
    atlasAggregatorService: AtlasAggregatorService
  ): ShardedAggregatorService = {
    val c = configManager.orElseGet(() => ConfigManager.dynamicConfigManager())
    val r = registry.orElseGet(() => new NoopRegistry)
    new ShardedAggregatorService(c, r, client, atlasAggregatorService)
  }

  @Bean
  def aggrRegistryEndpoint(service: AtlasAggregatorService): EndpointMapping = {
    new EndpointMapping("/aggregates", new SpectatorEndpoint(service.atlasRegistry))
  }
}
