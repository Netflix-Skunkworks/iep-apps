/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.iep.ses

import com.amazonaws.PredefinedClientConfigurations
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.NetflixEnvironment
import com.netflix.iep.service.Service

class AppModule extends AbstractModule {
  override def configure(): Unit = {
    val serviceBinder = Multibinder.newSetBinder(binder(), classOf[Service])
    serviceBinder.addBinding().to(classOf[SesMonitoringService])
  }

  @Provides
  def provideAwsSqsAsyncClient(): AmazonSQSAsync = {
    // TODO add to AwsClientFactory?
    AmazonSQSAsyncClient
      .asyncBuilder()
      .withRegion(NetflixEnvironment.region())
      .withCredentials(new DefaultAWSCredentialsProviderChain)
      .withClientConfiguration(PredefinedClientConfigurations.defaultConfig().withGzip(true))
      .build()
  }
}
