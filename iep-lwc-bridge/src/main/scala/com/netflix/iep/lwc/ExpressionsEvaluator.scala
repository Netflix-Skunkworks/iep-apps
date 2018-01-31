/*
 * Copyright 2014-2018 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicReference

import com.netflix.atlas.core.index.QueryIndex
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.spectator.atlas.impl.DataExpr.Aggregator
import com.netflix.spectator.atlas.impl.EvalPayload
import com.netflix.spectator.atlas.impl.Subscription
import com.netflix.spectator.atlas.impl.TagsValuePair

/**
  * Evaluate a set of data expressions from the LWC API service. This performs the same
  * basic role as the Evaluator class from the Spectator Atlas registry, but leverages the
  * query index from Atlas so it can scale to a much larger set of expressions.
  */
class ExpressionsEvaluator {
  import ExpressionsEvaluator._

  private val indexRef = new AtomicReference[QueryIndex[Subscription]](emptyIndex)

  /**
    * Synchronize the set of subscriptions for this evaluator with the specified list. Typically
    * the list will come from the `/expressions` endpoint of the LWC API service.
    */
  def sync(subs: SubscriptionList): Unit = {
    import scala.collection.JavaConverters._
    val index = QueryIndex.create(subs.asScala.map(toEntry).toList)

    // TODO: send diagnostic message about ignored expressions.
    // The top level fallback entries are removed to greatly reduce the cost. These entries
    // would need to get checked against every datapoint and are typically costly regex queries.
    // For a simple test remove the 46 top-level fallback expressions of 18.5k overall improved
    // throughput by 3x.
    indexRef.set(index.copy(entries = Nil))
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
    values.foreach { v =>
      val subs = index.matchingEntries(v.tags)
      subs.foreach { sub =>
        // TODO: avoid duplicate check of query when spectator 0.62.0 is released
        val aggr = aggregates.getOrElseUpdate(sub.getId, sub.dataExpr().aggregator())
        aggr.update(toPair(v))
      }
    }

    val metrics = new MetricList
    aggregates.toList.foreach {
      case (id, aggr) =>
        aggr.result().forEach { pair =>
          val m = new EvalPayload.Metric(id, pair.tags(), pair.value())
          metrics.add(m)
        }
    }

    new EvalPayload(timestamp, metrics)
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
}

object ExpressionsEvaluator {
  private val interpreter = Interpreter(QueryVocabulary.words)

  private val emptyIndex = QueryIndex.create[Subscription](Nil)

  def parseQuery(str: String): Query = {
    val stack = interpreter.execute(str).stack
    require(stack.lengthCompare(1) == 0, s"invalid query: $str")
    stack.head.asInstanceOf[Query]
  }
}
