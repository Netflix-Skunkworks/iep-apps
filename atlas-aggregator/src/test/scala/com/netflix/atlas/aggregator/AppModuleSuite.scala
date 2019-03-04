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

import javax.inject.Singleton
import com.google.inject.AbstractModule
import com.google.inject.ConfigurationException
import com.google.inject.Guice
import com.google.inject.Provider
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasConfig
import com.netflix.spectator.atlas.AtlasRegistry
import com.netflix.spectator.impl.AsciiSet
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class AppModuleSuite extends FunSuite {

  import AppModuleSuite._

  private val config = ConfigFactory.load()

  test("aggr registry only") {
    val injector = Guice.createInjector(new AppModule, new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(config)
      }
    })

    val aggr = injector.getInstance(classOf[AtlasRegistry])
    assert(aggr != null)

    intercept[ConfigurationException] {
      injector.getInstance(classOf[Registry])
    }
  }

  test("app and aggr registry") {
    val injector = Guice.createInjector(
      new AppModule,
      new AbstractModule {
        override def configure(): Unit = {
          bind(classOf[Config]).toInstance(config)
          bind(classOf[Registry]).toProvider(classOf[RegistryProvider])
        }
      }
    )

    val app = injector.getInstance(classOf[Registry])
    val aggr = injector.getInstance(classOf[AtlasRegistry])
    assert(app != aggr)
  }

  test("aggr config should use prefix") {
    val config = ConfigFactory.parseString("""
        |netflix.atlas.aggr.registry.atlas.uri = "test"
      """.stripMargin)
    val aggr = new AppModule.AggrConfig(config)
    assert(aggr.uri() === "test")
  }

  test("aggr config should allow ~") {
    val config = ConfigFactory.empty()
    val aggr = new AppModule.AggrConfig(config)
    val set = AsciiSet.fromPattern(aggr.validTagCharacters())

    // quick sanity check of the allowed values
    assert(set.contains('7'))
    assert(set.contains('c'))
    assert(set.contains('C'))
    assert(set.contains('~'))
    assert(set.contains('_'))
    assert(!set.contains('!'))
    assert(!set.contains('%'))
    assert(!set.contains('/'))
    assert(!set.contains(':'))
  }

  test("aggr config should use default for missing props") {
    val config = ConfigFactory.parseString("""
        |netflix.atlas.aggr.registry.atlas.uri = "test"
      """.stripMargin)
    val aggr = new AppModule.AggrConfig(config)
    assert(aggr.batchSize() === 10000)
  }
}

object AppModuleSuite {

  @Singleton
  class RegistryProvider extends Provider[Registry] {
    override def get(): Registry = {
      val cfg = new AtlasConfig {
        override def get(k: String): String = null
      }
      new AtlasRegistry(Clock.SYSTEM, cfg)
    }
  }
}
