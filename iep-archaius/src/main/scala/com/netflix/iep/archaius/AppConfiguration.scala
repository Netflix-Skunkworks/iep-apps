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
package com.netflix.iep.archaius

import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.util.Optional

@Configuration
class AppConfiguration {

  @Bean
  def dynamoDbClient(factory: AwsClientFactory): DynamoDbClient = {
    factory.getInstance(classOf[DynamoDbClient])
  }

  @Bean
  def dynamoService(client: DynamoDbClient): DynamoService = {
    new DynamoService(client)
  }

  @Bean
  def propertiesContext(registry: Optional[Registry]): PropertiesContext = {
    val r = registry.orElseGet(() => new NoopRegistry)
    new PropertiesContext(r)
  }
}
