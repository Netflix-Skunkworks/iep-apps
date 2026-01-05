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
package com.netflix.iep.archaius

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy

class MockDynamoDB extends InvocationHandler {

  var scanResponse: ScanResponse = _

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
    val client = proxy.asInstanceOf[DynamoDbClient]
    method.getName match {
      case "scan"          => scanResponse
      case "scanPaginator" => new ScanIterable(client, args(0).asInstanceOf[ScanRequest])
      case _               => throw new UnsupportedOperationException(method.toString)
    }
  }

  def client: DynamoDbClient = {
    val clsLoader = Thread.currentThread().getContextClassLoader
    val proxy = Proxy.newProxyInstance(clsLoader, Array(classOf[DynamoDbClient]), this)
    proxy.asInstanceOf[DynamoDbClient]
  }
}
