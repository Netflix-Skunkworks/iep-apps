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
package com.netflix.atlas.aggregator

import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.spectator.atlas.impl.QueryIndex
import com.netflix.spectator.impl.Cache

/** Cache implementation to use with the query index. */
class CaffeineCache[T] extends Cache[String, java.util.List[QueryIndex[T]]] {

  private val delegate = Caffeine
    .newBuilder()
    .maximumSize(10_000)
    .build[String, java.util.List[QueryIndex[T]]]()

  override def get(key: String): java.util.List[QueryIndex[T]] = {
    delegate.getIfPresent(key)
  }

  override def peek(key: String): java.util.List[QueryIndex[T]] = {
    throw new UnsupportedOperationException()
  }

  override def put(key: String, value: java.util.List[QueryIndex[T]]): Unit = {
    delegate.put(key, value)
  }

  override def computeIfAbsent(
    key: String,
    f: java.util.function.Function[String, java.util.List[QueryIndex[T]]]
  ): java.util.List[QueryIndex[T]] = {
    delegate.get(key, f)
  }

  override def clear(): Unit = {
    delegate.invalidateAll()
  }

  override def size(): Int = {
    delegate.estimatedSize().toInt
  }

  override def asMap(): java.util.Map[String, java.util.List[QueryIndex[T]]] = {
    java.util.Collections.emptyMap()
  }
}
