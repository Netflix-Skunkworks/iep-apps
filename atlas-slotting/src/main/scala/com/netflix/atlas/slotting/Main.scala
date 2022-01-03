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
package com.netflix.atlas.slotting

import com.google.inject.AbstractModule
import com.google.inject.Module
import com.google.inject.Provides
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.guice.BaseModule
import com.netflix.iep.guice.GuiceHelper
import com.netflix.iep.service.Service
import com.netflix.iep.service.ServiceManager
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.ec2.Ec2Client

import javax.inject.Singleton

object Main extends StrictLogging {

  private def isLocalEnv: Boolean = !sys.env.contains("EC2_INSTANCE_ID")

  private def getBaseModules: java.util.List[Module] = {
    val modules = {
      GuiceHelper.getModulesUsingServiceLoader
    }

    if (isLocalEnv) {
      // If we are running in a local environment, provide simple versions of registry
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
      modules.add(new ServerModule)

      val guice = new GuiceHelper
      guice.start(modules)
      guice.getInjector.getInstance(classOf[ServiceManager])
      guice.addShutdownHook()
    } catch {
      // Send exceptions to main log file instead of wherever STDERR is sent for the process
      case t: Throwable => logger.error("fatal error on startup", t)
    }
  }

  class ServerModule extends BaseModule {
    override def configure(): Unit = {
      val serviceBinder = Multibinder.newSetBinder(binder(), classOf[Service])
      serviceBinder.addBinding().to(classOf[SlottingService])
    }

    @Provides
    @Singleton
    protected def providesAmazonDynamoDB(factory: AwsClientFactory): DynamoDbClient = {
      factory.getInstance(classOf[DynamoDbClient])
    }

    @Provides
    @Singleton
    protected def providesAmazonEC2(factory: AwsClientFactory): Ec2Client = {
      factory.getInstance(classOf[Ec2Client])
    }

    @Provides
    @Singleton
    protected def providesAmazonAutoScaling(factory: AwsClientFactory): AutoScalingClient = {
      factory.getInstance(classOf[AutoScalingClient])
    }
  }
}
