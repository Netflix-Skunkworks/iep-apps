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
package com.netflix.iep.lwc

import com.netflix.iep.config.ConfigListener
import com.netflix.iep.config.DynamicConfigManager

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Utils
import com.netflix.spectator.atlas.impl.DataExpr.Aggregator
import com.netflix.spectator.atlas.impl.EvalPayload
import com.netflix.spectator.atlas.impl.QueryIndex
import com.netflix.spectator.atlas.impl.Subscription
import com.netflix.spectator.atlas.impl.TagsValuePair
import com.typesafe.scalalogging.StrictLogging

import java.util.Collections

/**
  * Evaluate a set of data expressions from the LWC API service. This performs the same
  * basic role as the Evaluator class from the Spectator Atlas registry, but leverages the
  * query index from Atlas so it can scale to a much larger set of expressions.
  */
class ExpressionsEvaluator(configMgr: DynamicConfigManager, registry: Registry)
    extends StrictLogging {

  import ExpressionsEvaluator.*

  @volatile private var subIdsToLog = Set.empty[String]

  configMgr.addListener(
    ConfigListener.forStringList(
      "netflix.iep.lwc.bridge.logging.subscriptions",
      vs => {
        import scala.jdk.CollectionConverters.*
        subIdsToLog = vs.asScala.toSet
      }
    )
  )

  private val subsAdded = registry.distributionSummary("lwc.syncExprsAdded")
  private val subsRemoved = registry.distributionSummary("lwc.syncExprsRemoved")

  private var subscriptions = Set.empty[Subscription]

  @volatile private[lwc] var index = QueryIndex.newInstance[Subscription](new NoopRegistry)

  private val statsMap = new ConcurrentHashMap[String, SubscriptionStats]()

  private val pendingMessages = new LinkedBlockingQueue[EvalPayload.Message]()

  /**
    * Synchronize the set of subscriptions for this evaluator with the specified list. Typically
    * the list will come from the `/expressions` endpoint of the LWC API service.
    */
  def sync(subs: SubscriptionList): Unit = synchronized {
    import scala.jdk.CollectionConverters.*

    val previous = subscriptions
    val current = subs.asScala.toSet
    val added = current.diff(previous)
    val removed = previous.diff(current)
    subscriptions = current

    subsAdded.record(added.size)
    subsRemoved.record(removed.size)

    added.foreach(s => index.add(s.dataExpr().query(), s))
    removed.foreach(s => index.remove(s.dataExpr().query(), s))
  }

  /**
    * Evaluate a set of datapoints and generate a payload for the `/evaluate` endpoint
    * of the LWC API service.
    */
  def eval(timestamp: Long, values: List[BridgeDatapoint]): EvalPayload = {
    val aggregates = collection.mutable.AnyRefMap.empty[String, Aggregator]
    values.filter(!_.value.isNaN).foreach { v =>
      val subs = index.findMatches(v.id)
      if (!subs.isEmpty) {
        val pair = new TagsValuePair(v.tagsMap, v.value)
        subs.forEach { sub =>
          Utils.computeIfAbsent(statsMap, sub.getId, (_: String) => SubscriptionStats(sub)).update()
          val aggr = aggregates.getOrElseUpdate(sub.getId, newAggregator(sub))
          aggr.update(pair)
          if (subIdsToLog.contains(sub.getId)) {
            logger.info(s"received value for ${sub.getId}, ts=$timestamp: $pair")
          }
        }
      }
    }

    val metrics = new MetricList
    aggregates.toList.foreach {
      case (id, aggr) =>
        aggr.result().forEach { pair =>
          val m = new EvalPayload.Metric(id, pair.tags(), pair.value())
          metrics.add(m)
          if (subIdsToLog.contains(id)) {
            logger.info(s"sending aggr value for $id, ts=$timestamp: $m")
          }
        }
    }

    val messages = new MessageList
    pendingMessages.drainTo(messages, 1000)
    new EvalPayload(timestamp, metrics, messages)
  }

  private def newAggregator(sub: Subscription): Aggregator = {
    val tags = sub.dataExpr().resultTags(Collections.emptyMap())
    sub.dataExpr().aggregator(tags, false)
  }

  /** Return some basic stats about the subscriptions that are being evaluated. */
  def stats: List[SubscriptionStats] = {
    val builder = List.newBuilder[SubscriptionStats]
    val iter = statsMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val ss = entry.getValue
      if (ss.age > OneHour) {
        iter.remove()
      } else {
        builder += ss
      }
    }
    builder.result()
  }

  /** Clear all internal state for this evaluator. */
  def clear(): Unit = synchronized {
    subscriptions.foreach(s => index.remove(s.dataExpr().query(), s))
    subscriptions = Set.empty[Subscription]
    statsMap.clear()
  }
}

object ExpressionsEvaluator {

  private val OneHour = 60 * 60 * 1000

  case class SubscriptionStats(
    sub: Subscription,
    lastUpdateTime: AtomicLong = new AtomicLong(),
    updateCount: AtomicLong = new AtomicLong()
  ) {

    def update(): Unit = {
      lastUpdateTime.set(System.currentTimeMillis())
      updateCount.incrementAndGet()
    }

    def age: Long = System.currentTimeMillis() - lastUpdateTime.get()
  }
}
