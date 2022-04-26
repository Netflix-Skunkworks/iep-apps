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
package com.netflix.atlas.persistence

import com.google.inject.AbstractModule
import com.google.inject.Module
import com.netflix.iep.guice.GuiceHelper
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

object Main extends StrictLogging {

  private def getBaseModules: Array[Module] = {
    val modules = GuiceHelper.getModulesUsingServiceLoader
    if (!sys.env.contains("NETFLIX_ENVIRONMENT")) {
      // If we are running in a local environment provide simple version of the config
      // binding. These bindings are normally provided by the final package
      // config for the app in the production setup.
      modules.add(new AbstractModule {
        override def configure(): Unit = {
          bind(classOf[Config]).toInstance(ConfigFactory.load())
          bind(classOf[Registry]).toInstance(new NoopRegistry)
        }
      })
    }
    modules.toArray(new Array[Module](0))
  }

  def main(args: Array[String]): Unit = {
    com.netflix.iep.guice.Main.run(args, getBaseModules: _*)
  }
}
