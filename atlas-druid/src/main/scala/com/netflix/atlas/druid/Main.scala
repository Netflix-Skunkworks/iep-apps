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
package com.netflix.atlas.druid

import com.google.inject.AbstractModule
import com.google.inject.Module
import com.netflix.iep.guice.GuiceHelper
import com.netflix.iep.service.ServiceManager
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Main {

  private val logger = LoggerFactory.getLogger(getClass)

  private def getBaseModules: java.util.List[Module] = {
    val modules = GuiceHelper.getModulesUsingServiceLoader
    if (!sys.env.contains("NETFLIX_ENVIRONMENT")) {
      // If we are running in a local environment provide simple versions of registry
      // and config bindings. These bindings are normally provided by the final package
      // config for the app in the production setup.
      modules.add(new AbstractModule {
        override def configure(): Unit = {
          bind(classOf[Registry]).toInstance(new NoopRegistry)
          bind(classOf[Config]).toInstance(ConfigFactory.load())
        }
      })
    }
    modules
  }

  def main(args: Array[String]): Unit = {
    try {
      val modules = getBaseModules
      val guice = new GuiceHelper
      guice.start(modules)
      guice.getInjector.getInstance(classOf[ServiceManager])
      guice.addShutdownHook()
    } catch {
      // Send exceptions to main log file instead of wherever STDERR is sent for the process
      case t: Throwable => logger.error("fatal error on startup", t)
    }
  }
}
