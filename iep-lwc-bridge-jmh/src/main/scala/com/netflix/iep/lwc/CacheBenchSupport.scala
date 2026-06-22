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
package com.netflix.iep.lwc

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.atlas.impl.QueryIndex
import com.netflix.spectator.impl.Cache

/** Shared construction for the cache benchmarks so the two benchmark classes do not drift. */
object CacheBenchSupport {

  /** Create a cache of the given type, using the same bounds the bridge query index uses. */
  def newCache(cacheType: String): Cache[String, QueryIndex.CacheValue[AnyRef]] = cacheType match {
    case "lfu" =>
      Cache.lfu[String, QueryIndex.CacheValue[AnyRef]](new NoopRegistry, "bench", 100, 1000)
    case "sampled" =>
      new SampledLfuCache[String, QueryIndex.CacheValue[AnyRef]](
        new NoopRegistry,
        "bench",
        100,
        1000
      )
    case "caffeine" =>
      new CaffeineCache[AnyRef]
    case "scoped" =>
      new ScopedValueCache[QueryIndex.CacheValue[AnyRef]]
    case other =>
      throw new IllegalArgumentException(s"unknown cacheType: $other")
  }

  /** Distinct tag values seen at a node; size controls churn relative to the cache bound. */
  def payloadKeys(keySpace: Int): Array[String] =
    Array.tabulate(keySpace)(i => s"app$i-main-v042")

  /** A cached value; contents are irrelevant to the cache mechanics being measured. */
  def emptyValue(): QueryIndex.CacheValue[AnyRef] =
    new QueryIndex.CacheValue[AnyRef](0L, new java.util.ArrayList[QueryIndex[AnyRef]]())
}
