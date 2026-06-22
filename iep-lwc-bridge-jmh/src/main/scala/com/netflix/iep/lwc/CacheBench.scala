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

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
  * Compares the query index cache implementations under the bridge's access pattern: many
  * threads (one per concurrent payload) hitting a single hot node's cache, where each payload
  * is a run of `batchSize` lookups of the same value (within-payload tag affinity) before
  * rotating to another value drawn from `keySpace` distinct values.
  *
  * `keySpace` relative to the cache bounds (lfu compaction=1000, caffeine maximumSize=10000)
  * controls churn:
  *   - 100   -> fits both, ~no eviction: isolates the steady-state read path
  *             (lfu LongAdder increment vs caffeine read-buffer + scheduled maintenance).
  *   - 2000  -> evicts in lfu, fits caffeine.
  *   - 20000 -> evicts in both: heavy create/evict churn.
  *
  * The interesting dimension is the thread count, since the production regression was a
  * concurrency effect. Sweep it to see how each scales:
  *
  * > iep-lwc-bridge-jmh/Jmh/run -wi 5 -i 10 -f1 -t1  .*CacheBench.*
  * > iep-lwc-bridge-jmh/Jmh/run -wi 5 -i 10 -f1 -t8  .*CacheBench.*
  * > iep-lwc-bridge-jmh/Jmh/run -wi 5 -i 10 -f1 -t32 .*CacheBench.*
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class CacheBench {

  @Param(Array("lfu", "sampled", "caffeine"))
  var cacheType: String = null

  @Param(Array("100", "2000", "20000"))
  var keySpace: Int = 0

  @Param(Array("1000"))
  var batchSize: Int = 0

  private var cache: Cache[String, QueryIndex.CacheValue[AnyRef]] = null
  private var keys: Array[String] = null
  private var value: QueryIndex.CacheValue[AnyRef] = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    cache = CacheBenchSupport.newCache(cacheType)
    keys = CacheBenchSupport.payloadKeys(keySpace)
    value = CacheBenchSupport.emptyValue()
  }

  @Benchmark
  def lookup(cursor: BatchCursor): AnyRef = {
    if (cursor.remaining <= 0) {
      cursor.key = keys(ThreadLocalRandom.current().nextInt(keySpace))
      cursor.remaining = batchSize
    }
    cursor.remaining -= 1
    val v = cache.get(cursor.key)
    if (v == null) {
      // Miss: populate as otherChecksComputeIfAbsent would on the first lookup of a value.
      cache.put(cursor.key, value)
      value
    } else {
      v
    }
  }
}

/** Per-thread cursor tracking the current payload's value and how many lookups remain in it. */
@State(Scope.Thread)
class BatchCursor {

  var remaining: Int = 0
  var key: String = null
}
