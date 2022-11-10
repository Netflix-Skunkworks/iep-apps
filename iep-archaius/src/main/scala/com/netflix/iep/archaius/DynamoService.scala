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
package com.netflix.iep.archaius

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import com.netflix.iep.service.AbstractService
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
  * Provides access to a dynamo client and a dedicated thread pool for executing
  * calls. Sample usage:
  *
  * ```
  * val future = dynamoService.execute { client =>
  *   client.scan(new ScanRequest().withTableName("foo"))
  * }
  * ```
  */
class DynamoService(client: DynamoDbClient, config: Config) extends AbstractService {

  private val nextId = new AtomicLong()

  private val pool = Executors.newFixedThreadPool(
    Runtime.getRuntime.availableProcessors(),
    (r: Runnable) => {
      new Thread(r, s"dynamo-db-${nextId.getAndIncrement()}")
    }
  )
  private val ec = ExecutionContext.fromExecutorService(pool)

  override def startImpl(): Unit = ()

  override def stopImpl(): Unit = ()

  def execute[T](task: DynamoDbClient => T): Future[T] = Future(task(client))(ec)
}
