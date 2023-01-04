/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.iep.lwc.fwd.admin

import akka.actor.ActorSystem
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.util.Optional

@Configuration
class AppConfiguration {

  @Bean
  def awsDynamoDBClient(factory: AwsClientFactory): DynamoDbClient = {
    factory.getInstance(classOf[DynamoDbClient])
  }

  @Bean
  def scalingPoliciesDao(
    config: Optional[Config],
    system: ActorSystem
  ): ScalingPoliciesDao = {
    val c = config.orElseGet(() => ConfigFactory.load())
    new ScalingPoliciesDaoImpl(c, system)
  }

  @Bean
  def expressionDetailsDao(
    config: Optional[Config],
    ddbClient: DynamoDbClient,
    registry: Optional[Registry]
  ): ExpressionDetailsDao = {
    val c = config.orElseGet(() => ConfigFactory.load())
    val r = registry.orElseGet(() => new NoopRegistry)
    new ExpressionDetailsDaoImpl(c, ddbClient, r)
  }

  @Bean
  def purger(
    config: Optional[Config],
    expressionDetailsDao: ExpressionDetailsDao,
    system: ActorSystem
  ): Purger = {
    val c = config.orElseGet(() => ConfigFactory.load())
    new PurgerImpl(c, expressionDetailsDao, system)
  }

  @Bean
  def markerService(
    config: Optional[Config],
    registry: Optional[Registry],
    expressionDetailsDao: ExpressionDetailsDao,
    system: ActorSystem
  ): MarkerService = {
    val c = config.orElseGet(() => ConfigFactory.load())
    val r = registry.orElseGet(() => new NoopRegistry)
    new MarkerServiceImpl(c, r, expressionDetailsDao, system)
  }

  @Bean
  def schemaValidation(): SchemaValidation = {
    new SchemaValidation
  }

  @Bean
  def exprInterpreter(config: Config): ExprInterpreter = {
    new ExprInterpreter(config)
  }

  @Bean
  def cwExprValidations(interpreter: ExprInterpreter, evaluator: Evaluator): CwExprValidations = {
    new CwExprValidations(interpreter, evaluator)
  }
}
