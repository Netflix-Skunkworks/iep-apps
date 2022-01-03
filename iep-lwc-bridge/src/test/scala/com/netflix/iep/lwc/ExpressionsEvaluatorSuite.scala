/*
 * Copyright 2014-2022 Netflix, Inc.
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
import com.netflix.spectator.atlas.impl.Subscription
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class ExpressionsEvaluatorSuite extends FunSuite {

  import scala.jdk.CollectionConverters._

  private val config = ConfigFactory.load()
  private val registry = new NoopRegistry

  // pick an arbitrary time
  private val timestamp = 42L * 60000L

  private def createSubs(exprs: String*): SubscriptionList = {
    val subs = exprs.zipWithIndex.map {
      case (expr, i) =>
        new Subscription()
          .withId(i.toString)
          .withFrequency(60000)
          .withExpression(expr)
    }
    subs.asJava
  }

  private def data(values: Double*): List[BridgeDatapoint] = {
    values.toList.zipWithIndex.map {
      case (v, i) =>
        val tags = Array("node", f"i-$i%02d")
        new BridgeDatapoint("cpu", tags, tags.length, timestamp, v)
    }
  }

  test("eval with no expressions") {
    val evaluator = new ExpressionsEvaluator(config, registry)
    val payload = evaluator.eval(timestamp, data(1.0))
    assertEquals(payload.getTimestamp, timestamp)
    assert(payload.getMetrics.isEmpty)
  }

  test("eval with single expression") {
    val evaluator = new ExpressionsEvaluator(config, registry)
    evaluator.sync(createSubs("node,i-00,:eq,:sum"))
    val payload = evaluator.eval(timestamp, data(1.0))
    assertEquals(payload.getTimestamp, timestamp)
    assertEquals(payload.getMetrics.size(), 1)

    val m = payload.getMetrics.get(0)
    assertEquals(m.getId, "0")
    assertEquals(m.getValue, 1.0)
  }

  test("eval with multiple datapoints for an aggregate") {
    val evaluator = new ExpressionsEvaluator(config, registry)
    evaluator.sync(createSubs("node,i-00,:eq,:sum"))
    val payload = evaluator.eval(timestamp, data(1.0) ::: data(4.0))
    assertEquals(payload.getTimestamp, timestamp)
    assertEquals(payload.getMetrics.size(), 1)

    val m = payload.getMetrics.get(0)
    assertEquals(m.getId, "0")
    assertEquals(m.getValue, 5.0)
  }

  test("eval with multiple datapoints ignores NaN values") {
    val evaluator = new ExpressionsEvaluator(config, registry)
    evaluator.sync(createSubs("node,i-00,:eq,:sum"))
    val payload = evaluator.eval(timestamp, data(7.0) ::: data(Double.NaN))
    assertEquals(payload.getTimestamp, timestamp)
    assertEquals(payload.getMetrics.size(), 1)

    val m = payload.getMetrics.get(0)
    assertEquals(m.getId, "0")
    assertEquals(m.getValue, 7.0)
  }

  test("eval with multiple expressions") {
    val evaluator = new ExpressionsEvaluator(config, registry)
    evaluator.sync(createSubs("node,i-00,:eq,:sum", "node,i-00,:eq,:max"))
    val payload = evaluator.eval(timestamp, data(1.0) ::: data(4.0))
    assertEquals(payload.getTimestamp, timestamp)
    assertEquals(payload.getMetrics.size(), 2)

    payload.getMetrics.asScala.foreach { m =>
      val expected = if (m.getId == "0") 5.0 else 4.0
      assertEquals(m.getValue, expected)
    }
  }

  test("sync: add/remove expressions") {
    val evaluator = new ExpressionsEvaluator(config, registry)
    evaluator.sync(createSubs("node,i-00,:eq,:sum"))
    var payload = evaluator.eval(timestamp, data(1.0) ::: data(4.0))
    assertEquals(payload.getMetrics.size(), 1)
    assertEquals(payload.getMetrics.get(0).getValue, 5.0)

    // Add expression
    evaluator.sync(createSubs("node,i-00,:eq,:sum", "node,i-00,:eq,:max"))
    payload = evaluator.eval(timestamp, data(1.0) ::: data(4.0))
    assertEquals(payload.getMetrics.size(), 2)

    // Remove expression
    evaluator.sync(createSubs("node,i-00,:eq,:max"))
    payload = evaluator.eval(timestamp, data(1.0) ::: data(4.0))
    assertEquals(payload.getMetrics.size(), 1)
    assertEquals(payload.getMetrics.get(0).getValue, 4.0)
  }

  test("sync: bad expressions".ignore) {
    val evaluator = new ExpressionsEvaluator(config, registry)
    evaluator.sync(createSubs("node,i-00,:re,:sum"))
    val payload = evaluator.eval(timestamp, data(1.0) ::: data(4.0))
    assert(payload.getMetrics.isEmpty)
    assertEquals(payload.getMessages.size(), 1)
  }
}
