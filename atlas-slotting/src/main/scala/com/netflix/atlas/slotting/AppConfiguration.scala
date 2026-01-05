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
package com.netflix.atlas.slotting

import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.ec2.Ec2Client

import java.util.Optional

@Configuration
class AppConfiguration {

  @Bean
  def slottingCache(): SlottingCache = {
    new SlottingCache()
  }

  @Bean
  def slottingService(
    config: Optional[Config],
    registry: Optional[Registry],
    asgClient: AutoScalingClient,
    ddbClient: DynamoDbClient,
    ec2Client: Ec2Client,
    cache: SlottingCache
  ): SlottingService = {
    val c = config.orElseGet(() => ConfigFactory.load())
    val r = registry.orElseGet(() => new NoopRegistry)
    new SlottingService(c, r, asgClient, ddbClient, ec2Client, cache)
  }

  @Bean
  def dynamoDb(factory: AwsClientFactory): DynamoDbClient = {
    factory.newInstance(classOf[DynamoDbClient])
  }

  @Bean
  def ec2(factory: AwsClientFactory): Ec2Client = {
    factory.newInstance(classOf[Ec2Client])
  }

  @Bean
  def autoScaling(factory: AwsClientFactory): AutoScalingClient = {
    factory.newInstance(classOf[AutoScalingClient])
  }
}
