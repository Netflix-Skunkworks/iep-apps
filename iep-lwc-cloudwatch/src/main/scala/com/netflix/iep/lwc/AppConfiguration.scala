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
package com.netflix.iep.lwc

import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.iep.aws2.AwsClientFactory
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
  def configStats: ConfigStats = {
    new ConfigStats
  }

  @Bean
  def forwardingService(
    config: Optional[Config],
    registry: Optional[Registry],
    configStats: ConfigStats,
    evaluator: Evaluator,
    factory: AwsClientFactory,
    system: ActorSystem
  ): ForwardingService = {
    val c = config.orElseGet(() => ConfigFactory.load())
    val r = registry.orElseGet(() => new NoopRegistry)
    new ForwardingService(c, r, configStats, evaluator, factory, system)
  }
}
