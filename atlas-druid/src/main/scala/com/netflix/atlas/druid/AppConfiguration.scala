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
package com.netflix.atlas.druid

import com.netflix.atlas.pekko.AccessLogger
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem

import java.util.Optional
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AppConfiguration {

  @Bean
  def metadataService: DruidMetadataService = {
    new DruidMetadataService
  }

  @Bean
  def druidClient(
    config: Optional[Config],
    connectionContext: Optional[HttpsConnectionContext],
    system: ActorSystem
  ): DruidClient = {
    val c = config.orElseGet(() => ConfigFactory.load())
    implicit val sys: ActorSystem = system
    val http = Http()
    // Use an injected connection context if one is provided, e.g. to support mTLS. This
    // allows an internal packaging to supply a client certificate without adding the
    // dependency here. Otherwise fall back to the default client context.
    val ctx = connectionContext.orElseGet(() => http.defaultClientHttpsContext)
    val client = http.superPool[AccessLogger](connectionContext = ctx)
    new DruidClient(c.getConfig("atlas.druid"), system, client)
  }
}
