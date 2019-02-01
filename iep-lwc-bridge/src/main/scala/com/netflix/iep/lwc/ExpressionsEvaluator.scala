/*
 * Copyright 2014-2019 Netflix, Inc.
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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import com.netflix.atlas.core.index.QueryIndex
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.spectator.api.Utils
import com.netflix.spectator.atlas.impl.DataExpr.Aggregator
import com.netflix.spectator.atlas.impl.EvalPayload
import com.netflix.spectator.atlas.impl.Subscription
import com.netflix.spectator.atlas.impl.TagsValuePair
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.inject.Singleton

/**
  * Evaluate a set of data expressions from the LWC API service. This performs the same
  * basic role as the Evaluator class from the Spectator Atlas registry, but leverages the
  * query index from Atlas so it can scale to a much larger set of expressions.
  */
@Singleton
class ExpressionsEvaluator @Inject()(config: Config) extends StrictLogging {
  import ExpressionsEvaluator._

  private val subIdsToLog = {
    import scala.collection.JavaConverters._
    config.getStringList("netflix.iep.lwc.bridge.logging.subscriptions").asScala.toSet
  }

  private val indexRef = new AtomicReference[QueryIndex[Subscription]](emptyIndex)

  private val statsMap = new ConcurrentHashMap[String, SubscriptionStats]()

  private val pendingMessages = new LinkedBlockingQueue[EvalPayload.Message]()

  /** Return the query index for this evaluator. */
  private[lwc] def index: QueryIndex[Subscription] = indexRef.get()

  /**
    * Synchronize the set of subscriptions for this evaluator with the specified list. Typically
    * the list will come from the `/expressions` endpoint of the LWC API service.
    */
  def sync(subs: SubscriptionList): Unit = {
    import scala.collection.JavaConverters._
    val index = QueryIndex.create(subs.asScala.map(toEntry).toList)

    // The top level fallback entries are removed to greatly reduce the cost. These entries
    // would need to get checked against every datapoint and are typically costly regex queries.
    // For a simple test remove the 46 top-level fallback expressions of 18.5k overall improved
    // throughput by 3x.
    index.entries.foreach { entry =>
      val sub = entry.value
      val expr = sub.getExpression
      val error = s"rejected expression [$expr], must have at least one equals clause"
      val msg = new EvalPayload.Message(
        sub.getId,
        new EvalPayload.DiagnosticMessage(EvalPayload.MessageType.error, error)
      )
      pendingMessages.offer(msg)
    }
    indexRef.set(index.copy(entries = Array.empty))
  }

  private def toEntry(sub: Subscription): QueryIndex.Entry[Subscription] = {
    // Convert from spectator Query object to atlas Query object needed for the index
    val atlasQuery = parseQuery(sub.dataExpr().query().toString)
    QueryIndex.Entry(atlasQuery, sub)
  }

  /**
    * Evaluate a set of datapoints and generate a payload for the `/evaluate` endpoint
    * of the LWC API service.
    */
  def eval(timestamp: Long, values: List[Datapoint]): EvalPayload = {
    val index = indexRef.get
    val aggregates = collection.mutable.AnyRefMap.empty[String, Aggregator]
    values.filter(!_.value.isNaN).foreach { v =>
      val subs = index.matchingEntries(v.tags)
      if (subs.nonEmpty) {
        val pair = toPair(v)
        subs.foreach { sub =>
          Utils.computeIfAbsent(statsMap, sub.getId, (_: String) => SubscriptionStats(sub)).update()
          val aggr = aggregates.getOrElseUpdate(sub.getId, newAggregator(sub))
          aggr.update(pair)
          if (subIdsToLog.contains(sub.getId)) {
            logger.info(s"received value for $sub: $pair")
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
            logger.info(s"sending aggr value for $id: $m")
          }
        }
    }

    val messages = new MessageList
    pendingMessages.drainTo(messages, 1000)
    new EvalPayload(timestamp, metrics, messages)
  }

  private def newAggregator(sub: Subscription): Aggregator = {
    val tags = sub.dataExpr().query().exactTags()
    sub.dataExpr().aggregator(tags, false)
  }

  private def toPair(d: Datapoint): TagsValuePair = {
    import scala.collection.JavaConverters._
    // Use custom java map wrapper from SmallHashMap if possible for improved
    // performance during the eval.
    val jmap = d.tags match {
      case m: SmallHashMap[_, _] => m.asInstanceOf[SmallHashMap[String, String]].asJavaMap
      case m                     => m.asJava
    }
    new TagsValuePair(jmap, d.value)
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
  def clear(): Unit = {
    indexRef.set(emptyIndex)
    statsMap.clear()
  }
}

object ExpressionsEvaluator {
  private val OneHour = 60 * 60 * 1000

  private val interpreter = Interpreter(QueryVocabulary.words)

  private val emptyIndex = QueryIndex.create[Subscription](Nil)

  def parseQuery(str: String): Query = {
    val stack = interpreter.execute(str).stack
    require(stack.lengthCompare(1) == 0, s"invalid query: $str")
    stack.head.asInstanceOf[Query]
  }

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
