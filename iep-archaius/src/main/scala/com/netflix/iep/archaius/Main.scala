/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.iep.archaius

import com.google.inject.Provides
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.config.ConfigManager
import com.netflix.iep.guice.BaseModule
import com.netflix.iep.service.Service
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import javax.inject.Singleton

object Main {

  def main(args: Array[String]): Unit = {
    com.netflix.iep.guice.Main.run(args, new ServerModule)
  }

  class ServerModule extends BaseModule {
    override def configure(): Unit = {
      bind(classOf[Config]).toInstance(ConfigManager.get())

      val serviceBinder = Multibinder.newSetBinder(binder(), classOf[Service])
      serviceBinder.addBinding().toConstructor(getConstructor(classOf[DynamoService]))
      bind(classOf[DynamoService])
      bind(classOf[PropertiesContext])
    }

    // Visibility of protected to avoid unused method warning from scala compiler
    @Provides
    @Singleton
    protected def providesDynamoDbClient(factory: AwsClientFactory): DynamoDbClient = {
      factory.getInstance(classOf[DynamoDbClient])
    }
  }
}
