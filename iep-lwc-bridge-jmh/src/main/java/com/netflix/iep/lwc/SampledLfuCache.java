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
package com.netflix.iep.lwc;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.Cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Prototype variant of spectator's package-private {@code LfuCache}, copied verbatim except for
 * one change: the per-read frequency update in {@link Pair#get()} is <b>sampled</b> rather than
 * applied on every hit.
 *
 * <p>The production cost being targeted is the {@code LongAdder} increment on every cache hit. On
 * the bridge's hot caches that is a high volume of release-semantics CASes (and repeated cold-start
 * cell growth as hot entries churn). Since LFU eviction only needs an approximate frequency
 * ordering, we can update 1-in-{@link #SAMPLE} hits and add {@link #SAMPLE} each time, so the
 * accumulated magnitude still approximates the true access count while cutting the atomic traffic
 * by ~{@code SAMPLE}x. Everything else (compaction, eviction, sizing) is identical to {@code
 * LfuCache} so the benchmark isolates the effect of the sampling alone.
 *
 * <p>TODO: This is a verbatim copy pinned to the current {@code LfuCache} internals and will drift
 * from upstream. Delete it once the sampling change is released in spectator and benchmark against
 * {@code Cache.lfu} directly.
 */
public class SampledLfuCache<K, V> implements Cache<K, V> {

  /** Sample 1-in-SAMPLE reads; must be a power of two so the mask works. */
  private static final int SAMPLE = 16;
  private static final int SAMPLE_MASK = SAMPLE - 1;

  private final Counter hits;
  private final Counter misses;
  private final Counter compactions;

  private final ConcurrentHashMap<K, Pair<V>> data;
  private final int baseSize;
  private final int compactionSize;

  // Collections that are reused for each compaction operation
  private final PriorityQueue<Snapshot<K>> mostFrequentItems;
  private final List<K> mostFrequentKeys;

  private final AtomicInteger size;

  private final Lock lock;

  public SampledLfuCache(Registry registry, String id, int baseSize, int compactionSize) {
    this.hits = registry.counter("spectator.cache.requests", "id", id, "result", "hit");
    this.misses = registry.counter("spectator.cache.requests", "id", id, "result", "miss");
    this.compactions = registry.counter("spectator.cache.compactions", "id", id);
    data = new ConcurrentHashMap<>();
    this.baseSize = baseSize;
    this.compactionSize = compactionSize;
    this.mostFrequentItems = new PriorityQueue<>(baseSize, SNAPSHOT_COMPARATOR);
    this.mostFrequentKeys = new ArrayList<>(baseSize);
    this.size = new AtomicInteger();
    this.lock = new ReentrantLock();
  }

  private void addIfMoreFrequent(K key, Pair<V> value) {
    long count = value.snapshot();
    if (mostFrequentItems.size() >= baseSize) {
      // Queue is full, add new item if it is more frequently used than the least
      // frequent item currently in the queue.
      Snapshot<K> leastFrequentItem = mostFrequentItems.peek();
      if (leastFrequentItem != null && count > leastFrequentItem.count()) {
        mostFrequentItems.poll();
        mostFrequentItems.offer(new Snapshot<>(key, count));
      }
    } else {
      mostFrequentItems.offer(new Snapshot<>(key, count));
    }
  }

  private void compact() {
    int numToRemove = size.get() - baseSize;
    if (numToRemove > 0) {
      mostFrequentItems.clear();
      mostFrequentKeys.clear();

      data.forEach(this::addIfMoreFrequent);
      mostFrequentItems.forEach(s -> mostFrequentKeys.add(s.get()));
      data.keySet().retainAll(mostFrequentKeys);

      size.set(data.size());
      compactions.increment();
    }
  }

  private void tryCompact() {
    if (lock.tryLock()) {
      try {
        compact();
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public V get(K key) {
    Pair<V> value = data.get(key);
    if (value == null) {
      misses.increment();
      return null;
    } else {
      hits.increment();
      return value.get();
    }
  }

  @Override public V peek(K key) {
    Pair<V> value = data.get(key);
    return value == null ? null : value.peek();
  }

  @Override public void put(K key, V value) {
    Pair<V> prev = data.put(key, new Pair<>(value));
    if (prev == null && size.incrementAndGet() > compactionSize) {
      tryCompact();
    }
  }

  @Override public V computeIfAbsent(K key, Function<K, V> f) {
    Pair<V> value = data.get(key);
    if (value == null) {
      misses.increment();
      Pair<V> tmp = new Pair<>(f.apply(key));
      value = data.putIfAbsent(key, tmp);
      if (value == null) {
        value = tmp;
        if (size.incrementAndGet() > compactionSize) {
          tryCompact();
        }
      }
    } else {
      hits.increment();
    }
    return value.get();
  }

  @Override public void clear() {
    size.set(0);
    data.clear();
  }

  @Override public int size() {
    return size.get();
  }

  @Override public Map<K, V> asMap() {
    return data.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().peek()));
  }

  private static class Pair<V> {
    private final V value;
    private final LongAdder count;

    Pair(V value) {
      this.value = value;
      this.count = new LongAdder();
    }

    V get() {
      // Sampled frequency update: only 1-in-SAMPLE reads touch the shared LongAdder, and each
      // such update adds SAMPLE so the running total still tracks the true access count in
      // expectation. ThreadLocalRandom is per-thread so the sampling decision adds no contention.
      if ((ThreadLocalRandom.current().nextInt() & SAMPLE_MASK) == 0) {
        count.add(SAMPLE);
      }
      return value;
    }

    V peek() {
      return value;
    }

    long snapshot() {
      return count.sum();
    }
  }

  private static class Snapshot<K> {
    private final K key;
    private final long count;

    Snapshot(K key, long count) {
      this.key = key;
      this.count = count;
    }

    K get() {
      return key;
    }

    long count() {
      return count;
    }
  }

  // Comparator for finding the least frequent items with the priority queue
  private static final Comparator<Snapshot<?>> SNAPSHOT_COMPARATOR =
      (a, b) -> Long.compare(b.count(), a.count());
}
