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

import com.netflix.spectator.atlas.impl.QueryIndex
import com.netflix.spectator.impl.Cache
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
  * Payload-level comparison of the cache implementations, including the scoped per-payload L1
  * (`scoped`). One unit of work is a whole payload: `BatchSize` lookups of a single value drawn
  * from `keySpace` distinct values (within-payload tag affinity), so throughput is payloads/s
  * (multiply by `BatchSize` for lookups/s).
  *
  * Unlike [[CacheBench]] this models the cost the scoped cache trades away: on a miss it spends
  * `missCost` Blackhole tokens to stand in for the regex recompute the cache exists to avoid. The
  * scoped cache starts every payload cold, so it always pays one recompute per payload; the shared
  * caches avoid it when the value is still resident from a prior payload. `keySpace` controls that
  * cross-payload reuse:
  *   - 100   -> values recur, shared caches keep them resident (scoped keeps recomputing).
  *   - 20000 -> values evicted between payloads, shared caches recompute too (reuse advantage gone).
  *
  * > iep-lwc-bridge-jmh/Jmh/run -wi 3 -w 3 -i 5 -r 3 -f1 -t32 .*CacheBatchBench.*
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class CacheBatchBench {

  @Param(Array("lfu", "sampled", "caffeine", "scoped"))
  var cacheType: String = null

  @Param(Array("100", "20000"))
  var keySpace: Int = 0

  // Blackhole tokens burned on a miss to stand in for the regex recompute. 0 isolates the pure
  // cache mechanism; a non-zero value reveals the cost of the scoped cache's lost cross-payload reuse.
  @Param(Array("0", "200"))
  var missCost: Int = 0

  private final val BatchSize = 1000
  private final val MaxL1 = 16

  private var cache: Cache[String, QueryIndex.CacheValue[AnyRef]] = null
  private var scoped: Boolean = false
  private var keys: Array[String] = null
  private var value: QueryIndex.CacheValue[AnyRef] = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    cache = CacheBenchSupport.newCache(cacheType)
    scoped = cacheType == "scoped"
    keys = CacheBenchSupport.payloadKeys(keySpace)
    value = CacheBenchSupport.emptyValue()
  }

  private def newL1Map(): java.util.Map[String, AnyRef] =
    new java.util.LinkedHashMap[String, AnyRef](MaxL1, 0.75f, true) {

      override def removeEldestEntry(eldest: java.util.Map.Entry[String, AnyRef]): Boolean =
        size() > MaxL1
    }

  private def processBatch(key: String): Unit = {
    var i = 0
    while (i < BatchSize) {
      val v = cache.get(key)
      if (v == null) {
        if (missCost > 0) Blackhole.consumeCPU(missCost.toLong)
        cache.put(key, value)
      }
      i += 1
    }
  }

  @Benchmark
  def payload(): Unit = {
    val key = keys(ThreadLocalRandom.current().nextInt(keySpace))
    if (scoped) {
      ScopedValue.where(ScopedValueCache.SCOPE, newL1Map()).run(() => processBatch(key))
    } else {
      processBatch(key)
    }
  }
}
