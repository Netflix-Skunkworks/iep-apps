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
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import munit.FunSuite
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

import java.time.Duration

class PublishClientSuite extends FunSuite {

  private val config = ConfigFactory.load()
  private val step = Duration.ofSeconds(1)
  private val stepSecs = step.getSeconds.toDouble

  private def makeClient(useStepCounter: Boolean = true): PublishClient = {
    val status = mock(classOf[LeaderStatus])
    when(status.hasLeadership).thenReturn(true)
    val cfg = new PublishConfig(
      config.withValue(
        "atlas.cloudwatch.poller.useStepCounter",
        ConfigValueFactory.fromAnyRef(useStepCounter)
      ),
      "http://localhost:7101/api/v1/publish",
      "http://localhost:7102/api/v1/expressions",
      "http://localhost:7102/api/v1/evaluate",
      status,
      new DefaultRegistry(),
      Some(step),
      Some(step)
    )
    new PublishClient(cfg)
  }

  test("updateStepCounter: value reported as rate (sum / stepSecs)") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 5.0)
    assertEquals(client.drainStepCounter(id), 5.0 / stepSecs)
  }

  test("updateStepCounter: accumulates multiple calls within same step") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 3.0)
    client.updateStepCounter(id, 2.0)
    assertEquals(client.drainStepCounter(id), 5.0 / stepSecs)
  }

  test("updateStepCounter: ref drained after poll, next step returns NaN") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 5.0)
    client.drainStepCounter(id) // drains the ref
    assert(client.drainStepCounter(id).isNaN)
  }

  test("updateStepCounter: NaN when no data has arrived") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    assert(client.drainStepCounter(id).isNaN)
  }

  test("updateStepCounter: value resumes after NaN step") {
    val client = makeClient()
    val id = client.publishRegistry.createId("test.metric")
    client.updateStepCounter(id, 4.0)
    assertEquals(client.drainStepCounter(id), 4.0 / stepSecs)
    assert(client.drainStepCounter(id).isNaN)
    client.updateStepCounter(id, 7.0)
    assertEquals(client.drainStepCounter(id), 7.0 / stepSecs)
  }

  test("updateStepCounter: independent ids accumulate separately") {
    val client = makeClient()
    val id1 = client.publishRegistry.createId("test.metric.a")
    val id2 = client.publishRegistry.createId("test.metric.b")
    client.updateStepCounter(id1, 10.0)
    client.updateStepCounter(id2, 20.0)
    assertEquals(client.drainStepCounter(id1), 10.0 / stepSecs)
    assertEquals(client.drainStepCounter(id2), 20.0 / stepSecs)
  }

  test("updateRate: routes to updateStepCounter when useStepCounter=true") {
    val client = makeClient(useStepCounter = true)
    val id = client.publishRegistry.createId("test.metric")
    client.updateRate(id, 5.0)
    assertEquals(client.drainStepCounter(id), 5.0 / stepSecs)
  }

  test("updateRate: routes to updateCounter when useStepCounter=false") {
    val client = makeClient(useStepCounter = false)
    val id = client.publishRegistry.createId("test.metric")
    client.updateRate(id, 5.0)
    // drainStepCounter returns NaN because the ref was never populated
    assert(client.drainStepCounter(id).isNaN)
    // counter holds the value directly
    assertEquals(client.publishRegistry.counter(id).count(), 5L)
  }
}
