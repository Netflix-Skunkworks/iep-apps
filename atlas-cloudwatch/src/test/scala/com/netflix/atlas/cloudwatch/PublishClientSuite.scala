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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.cloudwatch.poller.PublishClient
import com.netflix.atlas.cloudwatch.poller.PublishConfig
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Id
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

import java.time.Duration
import scala.jdk.StreamConverters.*

class PublishClientSuite extends FunSuite {

  private val config = ConfigFactory.load()
  private val step = Duration.ofSeconds(1)
  private val stepSecs = step.getSeconds.toDouble

  private def makeClient(): PublishClient = {
    val status = mock(classOf[LeaderStatus])
    when(status.hasLeadership).thenReturn(true)
    val cfg = new PublishConfig(
      config,
      null,
      null,
      null,
      status,
      new DefaultRegistry(),
      Some(step),
      Some(step)
    )
    new PublishClient(cfg)
  }

  // Triggers PolledMeter poll cycle and returns the measured value for `id`,
  // or NaN if the meter is absent or produced NaN (no data this step).
  private def measure(client: PublishClient, id: Id): Double = {
    client.publishRegistry
      .measurements()
      .toScala(LazyList)
      .find(_.id() == id)
      .map(_.value())
      .getOrElse(Double.NaN)
  }

  test("updateStepCounter: value reported as rate (sum / stepSecs)") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 5.0)
    assertEquals(measure(client, id), 5.0 / stepSecs)
  }

  test("updateStepCounter: accumulates multiple calls within same step") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 3.0)
    client.updateStepCounter(id, 2.0)
    assertEquals(measure(client, id), 5.0 / stepSecs)
  }

  test("updateStepCounter: ref drained after poll, next step returns NaN") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 5.0)
    measure(client, id) // drains the ref
    assert(measure(client, id).isNaN)
  }

  test("updateStepCounter: NaN when no data arrives in a step") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    // Register the meter first, then drain without ever calling updateStepCounter
    client.updateStepCounter(id, 1.0)
    measure(client, id) // drain
    assert(measure(client, id).isNaN)
  }

  test("updateStepCounter: value resumes after NaN step") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 4.0)
    assertEquals(measure(client, id), 4.0 / stepSecs) // drains
    assert(measure(client, id).isNaN) // no data → NaN
    client.updateStepCounter(id, 7.0)
    assertEquals(measure(client, id), 7.0 / stepSecs) // value resumes
  }

  test("updateStepCounter: independent ids accumulate separately") {
    val client = makeClient()
    val id1 = client.publishRegistry.createId("test.metric.a")
    val id2 = client.publishRegistry.createId("test.metric.b")
    client.updateStepCounter(id1, 10.0)
    client.updateStepCounter(id2, 20.0)
    assertEquals(measure(client, id1), 10.0 / stepSecs)
    assertEquals(measure(client, id2), 20.0 / stepSecs)
  }
}
