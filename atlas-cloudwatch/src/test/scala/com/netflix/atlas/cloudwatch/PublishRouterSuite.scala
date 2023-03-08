/*
 * Copyright 2014-2023 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.netflix.atlas.akka.AkkaHttpClient
import com.netflix.atlas.core.model.Datapoint
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.MockitoSugar.mock

class PublishRouterSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  var registry: Registry = null
  val config = ConfigFactory.load()
  val tagger = new NetflixTagger(config.getConfig("atlas.cloudwatch.tagger"))
  val httpClient = mock[AkkaHttpClient]

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
  }

  test("initialize") {
    val router = new PublishRouter(config, registry, tagger, httpClient)
    assertEquals(router.mainQueue.uri, "https://publish-main.foo.com/api/v1/publish")
    assertEquals(router.accountMap.size, 3)
    assertEquals(
      router.accountMap.get("1").get.uri,
      "https://publish-stackA.foo.com/api/v1/publish"
    )
    assertEquals(
      router.accountMap.get("2").get.uri,
      "https://publish-stackA.foo.com/api/v1/publish"
    )
    assertEquals(
      router.accountMap.get("3").get.uri,
      "https://publish-stackB.foo.com/api/v1/publish"
    )
    router.shutdown()
  }

  // NOTE: These are flaky. Either we need a queue factory that we can inject or figure out some way to pause the
  // Akka system. For now, routing is simple enough I'm not worried about testing.
//  test("publish main") {
//    val dp = Datapoint(Map("nf.account" -> "42"), timestamp, 42.0)
//    val router = spy(new PublishRouter(config, registry, tagger, httpClient))
//    router.shutdown()
//    router.publish(dp)
//    assertEquals(router.mainQueue.publishQueue.size, 1)
//  }
//
//  test("publish stackA") {
//    val dp = Datapoint(Map("nf.account" -> "1"), timestamp, 42.0)
//    val router = new PublishRouter(config, registry, tagger, httpClient)
//    router.shutdown()
//    router.publish(dp)
//    assertEquals(router.accountMap.get("1").get.publishQueue.size, 1)
//  }

  test("publish missing account tag") {
    val dp = Datapoint(Map("no" -> "account"), 1677628800000L, 42.0)
    val router = new PublishRouter(config, registry, tagger, httpClient)
    router.shutdown()
    router.publish(dp)
    assertEquals(
      registry.counter("atlas.cloudwatch.queue.dps.dropped", "reason", "missingAccount").count(),
      1L
    )
  }

}
