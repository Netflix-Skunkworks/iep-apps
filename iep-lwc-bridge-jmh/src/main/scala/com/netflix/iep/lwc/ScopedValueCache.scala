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

import com.netflix.spectator.impl.Cache

/**
  * Per-payload L1 cache backed by a [[java.lang.ScopedValue]]. The map is bound for the duration
  * of processing a single payload (one source's datapoints) on one thread, then discarded when the
  * scope exits. Because it is both thread-confined and scope-confined there is no sharing and so no
  * locking or shared writes at all — the opposite end of the spectrum from the long-lived shared
  * caches. It exploits within-payload tag affinity (the same value is looked up many times) but
  * gives up cross-payload reuse: every payload starts cold.
  *
  * The backing map must be bound by the caller before use, e.g.:
  * {{{ ScopedValue.where(ScopedValueCache.SCOPE, newMap).run(() => process(payload)) }}}
  */
class ScopedValueCache[V] extends Cache[String, V] {

  override def get(key: String): V =
    ScopedValueCache.SCOPE.get().get(key).asInstanceOf[V]

  override def peek(key: String): V =
    ScopedValueCache.SCOPE.get().get(key).asInstanceOf[V]

  override def put(key: String, value: V): Unit =
    ScopedValueCache.SCOPE.get().put(key, value.asInstanceOf[AnyRef])

  override def computeIfAbsent(key: String, f: java.util.function.Function[String, V]): V = {
    val map = ScopedValueCache.SCOPE.get()
    val existing = map.get(key)
    if (existing != null) {
      existing.asInstanceOf[V]
    } else {
      val v = f.apply(key)
      map.put(key, v.asInstanceOf[AnyRef])
      v
    }
  }

  override def clear(): Unit = ScopedValueCache.SCOPE.get().clear()

  override def size(): Int = ScopedValueCache.SCOPE.get().size()

  override def asMap(): java.util.Map[String, V] = java.util.Collections.emptyMap()
}

object ScopedValueCache {

  /** Bound per payload to a small, thread-confined map that lives only for that payload. */
  val SCOPE: ScopedValue[java.util.Map[String, AnyRef]] = ScopedValue.newInstance()
}
