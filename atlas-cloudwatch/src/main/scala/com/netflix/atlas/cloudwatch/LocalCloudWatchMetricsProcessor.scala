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
package com.netflix.atlas.cloudwatch

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newCacheEntry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future

/**
  * A local implementation of the metrics processor mean for unit testing or experimenting with
  * Cloud Watch data.
  *
  * @param config
  *     Non-null config to pull settings from.
  * @param registry
  *     Non-null registry to report metrics.
  * @param tagger
  *     The non-null tagger to use.
  * @param rules
  *     Non-null rules set to use for matching incoming data.
  * @param publishRouter
  *     The non-null publisher to report to.
  * @param system
  *     The Pekko system we use for scheduling.
  */
class LocalCloudWatchMetricsProcessor(
  config: Config,
  registry: Registry,
  rules: CloudWatchRules,
  tagger: Tagger,
  publishRouter: PublishRouter,
  debugger: CloudWatchDebugger
)(override implicit val system: ActorSystem)
    extends CloudWatchMetricsProcessor(config, registry, rules, tagger, publishRouter, debugger) {

  implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  //                                               writeTS, expSec, data
  private val cache = new ConcurrentHashMap[Long, (Long, Long, Array[Byte])]
  private val lastPoll = new ConcurrentHashMap[String, AtomicLong]

  override protected[cloudwatch] def updateCache(
    datapoint: FirehoseMetric,
    category: MetricCategory,
    receivedTimestamp: Long
  ): Future[Unit] = Future {
    val hash = datapoint.xxHash
    val existing = cache.get(hash)
    val cacheEntry = if (existing != null) {
      val (ts, exp, data) = existing
      if (ts + (exp * 1000) > receivedTimestamp) {
        insertDatapoint(data, datapoint, category, receivedTimestamp)
      } else {
        newCacheEntry(datapoint, category, receivedTimestamp)
      }
    } else {
      newCacheEntry(datapoint, category, receivedTimestamp)
    }

    cache.put(hash, (receivedTimestamp, expSeconds(category.period), cacheEntry.toByteArray))
  }

  override protected[cloudwatch] def publish(scrapeTimestamp: Long): Future[NotUsed] = {
    val iterator = cache.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val hash = entry.getKey
      val (ts, exp, data) = entry.getValue
      if (ts + (exp * 1000) < scrapeTimestamp) {
        cache.remove(hash)
      } else {
        sendToRouter(hash, data, scrapeTimestamp)
      }
    }
    Future.successful(NotUsed)
  }

  override protected[cloudwatch] def delete(key: Any): Unit = {
    cache.remove(key)
  }

  override protected[cloudwatch] def lastSuccessfulPoll(id: String): Long = {
    val lastPoll = this.lastPoll.get(id)
    if (lastPoll == null) 0L else lastPoll.get()
  }

  override protected[cloudwatch] def updateLastSuccessfulPoll(id: String, timestamp: Long): Unit = {
    lastPoll.computeIfAbsent(id, _ => new AtomicLong(0L)).set(timestamp)
  }

  private[cloudwatch] def inject(
    key: Long,
    payload: Array[Byte],
    timestamp: Long,
    exp: Long
  ): Unit = {
    cache.put(key, (timestamp, exp, payload))
  }

  override protected[cloudwatch] def updateCache(
    key: Any,
    prev: CloudWatchCacheEntry,
    entry: CloudWatchCacheEntry,
    expiration: Long
  ): Unit = {
    cache.put(key.asInstanceOf[Long], (0, expiration, entry.toByteArray))
  }
}
