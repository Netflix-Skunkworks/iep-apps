/*
 * Copyright 2014-2019 Netflix, Inc.
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

import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton
import com.google.inject.AbstractModule
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.admin.HttpEndpoint
import com.netflix.iep.admin.endpoints.SpectatorEndpoint
import com.netflix.iep.admin.guice.AdminModule
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.Service
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasConfig
import com.netflix.spectator.atlas.AtlasRegistry
import com.typesafe.config.Config

class AppModule extends AbstractModule {

  import AppModule._

  override def configure(): Unit = {
    val serviceBinder = Multibinder.newSetBinder(binder(), classOf[Service])
    serviceBinder.addBinding().to(classOf[AtlasAggregatorService])

    bind(classOf[AtlasRegistry]).toProvider(classOf[AtlasRegistryProvider]).asEagerSingleton()

    AdminModule
      .endpointsBinder(binder())
      .addBinding("/registry")
      .toProvider(classOf[EndpointProvider])
  }
}

object AppModule {

  class AggrConfig(config: Config, registry: Registry) extends AtlasConfig {

    override def get(k: String): String = {
      val prop = s"netflix.atlas.aggr.registry.$k"
      if (config.hasPath(prop)) config.getString(prop) else null
    }

    override def commonTags(): java.util.Map[String, String] = {
      val tags = new java.util.HashMap[String, String]()
      tags.put("atlas.aggr", config.getString("netflix.iep.env.instance-id"))
      tags
    }

    override def debugRegistry(): Registry = registry
  }

  @Singleton
  class AtlasAggregatorService @Inject()(registry: AtlasRegistry) extends AbstractService {
    override def startImpl(): Unit = {
      registry.start()
    }

    override def stopImpl(): Unit = {
      registry.stop()
    }
  }

  @Singleton
  class AtlasRegistryProvider @Inject()(config: Config, registry: Registry)
      extends Provider[AtlasRegistry] {
    override def get(): AtlasRegistry = {
      val cfg = new AggrConfig(config, registry)
      new AtlasRegistry(Clock.SYSTEM, cfg)
    }
  }

  @Singleton
  class EndpointProvider @Inject()(registry: AtlasRegistry) extends Provider[HttpEndpoint] {
    override def get(): HttpEndpoint = new SpectatorEndpoint(registry)
  }
}
