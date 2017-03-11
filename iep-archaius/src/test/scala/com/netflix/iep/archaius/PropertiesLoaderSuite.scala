/*
 * Copyright 2014-2017 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuiteLike


class PropertiesLoaderSuite extends TestKit(ActorSystem())
  with ImplicitSender
  with FunSuiteLike
  with BeforeAndAfterAll {

  val config = ConfigFactory.parseString(
    """
      |netflix.iep.archaius.table = "test"
    """.stripMargin)

  val clock = new ManualClock()
  val registry = new DefaultRegistry(clock)
  val propContext = new PropertiesContext(registry)

  val ddb = new MockDynamoDB
  val service = new DynamoService(ddb.client, config)

  val items = newItems("foo-main", Map("a" -> "b", "1" -> "2"))
  items.addAll(newItems("bar-main", Map("c" -> "d")))
  ddb.scanResult = new ScanResult().withItems(items)

  val ref = TestActorRef(new PropertiesLoader(config, propContext, service))

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
    assert(propContext.getAll === List(
      PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L),
      PropertiesApi.Property("foo-main::1", "foo-main", "1", "2", 12345L),
      PropertiesApi.Property("bar-main::c", "bar-main", "c", "d", 12345L)
    ))
  }

  test("update") {
    val items = newItems("foo-main", Map("a" -> "b"))
    items.addAll(newItems("bar-main", Map("c" -> "d")))
    ddb.scanResult = new ScanResult().withItems(items)

    waitForUpdate()

    assert(propContext.getAll === List(
      PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L),
      PropertiesApi.Property("bar-main::c", "bar-main", "c", "d", 12345L)
    ))
  }

  private def newItems(cluster: String, props: Map[String, String]): Items = {
    val items = new java.util.ArrayList[AttrMap]()
    props.foreach { case (k, v) =>
      val prop = PropertiesApi.Property(s"$cluster::$k", cluster, k, v, 12345L)
      val value = new AttributeValue().withS(Json.encode(prop))
      val timestamp = new AttributeValue().withS("12345")
      val m = new java.util.HashMap[String, AttributeValue]()
      m.put("data", value)
      m.put("timestamp", timestamp)
      items.add(m)
    }
    items
  }
}
