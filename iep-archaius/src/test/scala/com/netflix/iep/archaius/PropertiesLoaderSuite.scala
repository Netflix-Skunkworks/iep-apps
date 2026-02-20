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
package com.netflix.iep.archaius

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.ImplicitSender
import org.apache.pekko.testkit.TestActorRef
import org.apache.pekko.testkit.TestKitBase
import com.netflix.atlas.json3.Json
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

class PropertiesLoaderSuite extends FunSuite with TestKitBase with ImplicitSender {

  implicit val system: ActorSystem = ActorSystem()

  private val config: Config = ConfigFactory.parseString("""
      |netflix.iep.archaius.table = "test"
    """.stripMargin)

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)
  private val propContext = new PropertiesContext(registry)

  private val ddb = new MockDynamoDB
  private val service = new DynamoService(ddb.client)

  private val items = newItems("foo-main", Map("a" -> "b", "1" -> "2"))
  items.addAll(newItems("bar-main", Map("c" -> "d")))

  ddb.scanResponse = ScanResponse
    .builder()
    .items(items)
    .build()

  private val ref = TestActorRef(new PropertiesLoader(config, propContext, service))

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def waitForUpdate(): Unit = {
    val latch = propContext.latch
    ref ! PropertiesLoader.Tick
    latch.await()
  }

  test("init") {
    waitForUpdate()
    assert(propContext.initialized)
    assertEquals(
      propContext.getAll,
      List(
        PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L),
        PropertiesApi.Property("foo-main::1", "foo-main", "1", "2", 12345L),
        PropertiesApi.Property("bar-main::c", "bar-main", "c", "d", 12345L)
      )
    )
  }

  test("update") {
    val items = newItems("foo-main", Map("a" -> "b"))
    items.addAll(newItems("bar-main", Map("c" -> "d")))
    ddb.scanResponse = ScanResponse.builder().items(items).build()

    waitForUpdate()

    assertEquals(
      propContext.getAll,
      List(
        PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L),
        PropertiesApi.Property("bar-main::c", "bar-main", "c", "d", 12345L)
      )
    )
  }

  private def newItems(cluster: String, props: Map[String, String]): Items = {
    val items = new java.util.ArrayList[AttrMap]()
    props.foreach {
      case (k, v) =>
        val prop = PropertiesApi.Property(s"$cluster::$k", cluster, k, v, 12345L)
        val value = AttributeValue.builder().s(Json.encode(prop)).build()
        val timestamp = AttributeValue.builder().n("12345").build()
        val m = new java.util.HashMap[String, AttributeValue]()
        m.put("data", value)
        m.put("timestamp", timestamp)
        items.add(m)
    }
    items
  }
}
