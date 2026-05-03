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

import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKitBase

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class LogsPublishQueueSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private def sampleLog(msg: String = "test"): OtelLog =
    OtelLog(System.currentTimeMillis(), msg, "INFO", "test.logger", Map("env" -> "test"))

  private def makeQueue(
    sendLog: OtelLog => Unit,
    registry: Registry = new DefaultRegistry(),
    queueSize: Int = 1000,
    parallelism: Int = 4
  ): LogsPublishQueue = {
    val cfg = ConfigFactory.parseString(
      s"""atlas.cloudwatch.logs.queue.queueSize = $queueSize
         |atlas.cloudwatch.logs.queue.parallelism = $parallelism""".stripMargin
    )
    new LogsPublishQueue(cfg, registry, sendLog)
  }

  test("log is delivered to sendLog") {
    val received = new LinkedBlockingQueue[OtelLog]()
    val queue = makeQueue(log => received.put(log))

    val log = sampleLog("hello")
    queue.send(log)

    val delivered = received.poll(10, TimeUnit.SECONDS)
    assertNotEquals(delivered, null)
    assertEquals(delivered.message, "hello")
  }

  test("multiple logs are all delivered in order") {
    val received = new LinkedBlockingQueue[String]()
    val queue = makeQueue(log => received.put(log.message))

    (1 to 5).foreach(i => queue.send(sampleLog(s"msg-$i")))

    val messages = (1 to 5).map(_ => received.poll(10, TimeUnit.SECONDS))
    assertEquals(messages.toList, List("msg-1", "msg-2", "msg-3", "msg-4", "msg-5"))
  }

  test("sent counter increments on success") {
    val registry = new DefaultRegistry()
    val received = new LinkedBlockingQueue[OtelLog]()
    val queue = makeQueue(log => received.put(log), registry)

    queue.send(sampleLog())
    received.poll(10, TimeUnit.SECONDS)
    // give stream time to record the counter after sendLog returns
    Thread.sleep(200)

    assertEquals(registry.counter("atlas.cloudwatch.logs.queue.sent").count(), 1L)
  }

  test("sendLog exception is swallowed and dropped counter increments") {
    val registry = new DefaultRegistry()
    val successReceived = new LinkedBlockingQueue[OtelLog]()
    val callCount = new AtomicInteger(0)

    val queue = makeQueue(
      log => {
        if (callCount.incrementAndGet() == 1)
          throw new RuntimeException("TCP error")
        else
          successReceived.put(log)
      },
      registry
    )

    queue.send(sampleLog("will-fail"))
    queue.send(sampleLog("will-succeed"))

    val delivered = successReceived.poll(10, TimeUnit.SECONDS)
    assertNotEquals(delivered, null)
    assertEquals(delivered.message, "will-succeed")

    Thread.sleep(200)
    assertEquals(registry.counter("atlas.cloudwatch.logs.queue.dropped").count(), 1L)
    assertEquals(registry.counter("atlas.cloudwatch.logs.queue.sent").count(), 1L)
  }

  test("send does not throw when queue is full") {
    // Use a tiny queue and a slow sender to saturate it
    val queue = makeQueue(
      _ => Thread.sleep(5000),
      queueSize = 1
    )

    // Should not throw even when the queue is saturated
    (1 to 20).foreach(_ => queue.send(sampleLog()))
  }
}
