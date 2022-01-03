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
package com.netflix.iep.lwc.fwd.admin

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.service.Service
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import javax.inject.Singleton

class AppModule extends AbstractModule {
  override def configure(): Unit = {
    val serviceBinder = Multibinder.newSetBinder(binder(), classOf[Service])
    serviceBinder.addBinding().to(classOf[MarkerServiceImpl])
    bind(classOf[MarkerService]).to(classOf[MarkerServiceImpl])
    bind(classOf[Purger]).to(classOf[PurgerImpl])
    bind(classOf[ExpressionDetailsDao]).to(classOf[ExpressionDetailsDaoImpl])
    bind(classOf[ScalingPoliciesDao]).to(classOf[ScalingPoliciesDaoImpl])
  }

  // Visibility of protected to avoid unused method warning from scala compiler
  @Provides
  @Singleton
  protected def awsDynamoDBClient(factory: AwsClientFactory): DynamoDbClient = {
    factory.getInstance(classOf[DynamoDbClient])
  }
}
