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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKitBase
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ts
import com.netflix.atlas.core.model.Datapoint
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.MockitoSugar.mock

class PublishRouterSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private val registry: Registry = new DefaultRegistry()
  private val config = ConfigFactory.load()
  private val tagger = new NetflixTagger(config.getConfig("atlas.cloudwatch.tagger"))
  private val httpClient = mock[PekkoHttpClient]
  var leaderStatus: LeaderStatus = mock[LeaderStatus]
  private val router = new PublishRouter(config, registry, tagger, httpClient, leaderStatus)

  test("initialize") {
    assertEquals(router.mainQueue.uri, "https://publish-main.us-east-1.foo.com/api/v1/publish")
    assertEquals(router.accountMap.size, 3)
    router.shutdown()
  }

  test("publish main same region") {
    val dp = Datapoint(Map("nf.account" -> "42", "nf.region" -> "us-east-1"), ts, 42.0)
    val queue = router.getQueue(dp).get
    assertEquals(queue.uri, "https://publish-main.us-east-1.foo.com/api/v1/publish")
    router.shutdown()
  }

  test("publish main same any region") {
    val dp = Datapoint(Map("nf.account" -> "42", "nf.region" -> "ap-south-1"), ts, 42.0)
    val queue = router.getQueue(dp).get
    assertEquals(queue.uri, "https://publish-main.us-east-1.foo.com/api/v1/publish")
    router.shutdown()
  }

  test("publish stackA acct 1 default") {
    val dp = Datapoint(Map("nf.account" -> "1", "nf.region" -> "us-east-1"), ts, 42.0)
    val queue = router.getQueue(dp).get
    assertEquals(queue.uri, "https://publish-stackA.us-east-1.foo.com/api/v1/publish")
    router.shutdown()
  }

  test("publish stackA acct 2 default") {
    val dp = Datapoint(Map("nf.account" -> "2", "nf.region" -> "us-east-1"), ts, 42.0)
    val queue = router.getQueue(dp).get
    assertEquals(queue.uri, "https://publish-stackA.us-east-1.foo.com/api/v1/publish")
    router.shutdown()
  }

  test("publish stackA us-west") {
    val dp = Datapoint(Map("nf.account" -> "1", "nf.region" -> "us-west-1"), ts, 42.0)
    val queue = router.getQueue(dp).get
    assertEquals(queue.uri, "https://publish-stackA.us-west-1.foo.com/api/v1/publish")
    router.shutdown()
  }

  test("publish stackB us-west") {
    val dp = Datapoint(Map("nf.account" -> "3", "nf.region" -> "us-west-1"), ts, 42.0)
    val queue = router.getQueue(dp).get
    assertEquals(queue.uri, "https://publish-stackB.us-east-1.foo.com/api/v1/publish")
    router.shutdown()
  }

  test("publish missing account tag") {
    val dp = Datapoint(Map("no" -> "account"), 1677628800000L, 42.0)
    val router = new PublishRouter(config, registry, tagger, httpClient, leaderStatus)
    router.shutdown()
    router.publish(dp)
    assertEquals(
      registry.counter("atlas.cloudwatch.queue.dps.dropped", "reason", "missingAccount").count(),
      1L
    )
  }

}
