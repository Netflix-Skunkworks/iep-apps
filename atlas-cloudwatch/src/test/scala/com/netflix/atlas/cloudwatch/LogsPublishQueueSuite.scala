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

class LogsPublishQueueSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private def sampleLog(msg: String = "test"): OtelLog =
    OtelLog(System.currentTimeMillis(), msg, "INFO", "test.logger", Map("env" -> "test"))

  private def makeQueue(
    tcpSendBatch: Seq[OtelLog] => Unit,
    registry: Registry = new DefaultRegistry(),
    queueSize: Int = 1000,
    parallelism: Int = 4
  ): LogsPublishQueue = {
    val cfg = ConfigFactory.parseString(
      s"""atlas.cloudwatch.logs.queue.queueSize = $queueSize
         |atlas.cloudwatch.logs.queue.parallelism = $parallelism""".stripMargin
    )
    new LogsPublishQueue(cfg, registry, tcpSendBatch)
  }

  test("batch is delivered to sendBatch as a single call") {
    val received = new LinkedBlockingQueue[Seq[OtelLog]]()
    val queue = makeQueue(batch => received.put(batch))

    val logs = List(sampleLog("a"), sampleLog("b"), sampleLog("c"))
    queue.sendBatch(logs)

    val delivered = received.poll(10, TimeUnit.SECONDS)
    assertNotEquals(delivered, null)
    assertEquals(delivered.map(_.message), List("a", "b", "c"))
  }

  test("logs within a batch are delivered in order") {
    val received = new LinkedBlockingQueue[String]()
    val queue = makeQueue(batch => batch.foreach(log => received.put(log.message)))

    queue.sendBatch((1 to 5).map(i => sampleLog(s"msg-$i")))

    val messages = (1 to 5).map(_ => received.poll(10, TimeUnit.SECONDS))
    assertEquals(messages.toList, List("msg-1", "msg-2", "msg-3", "msg-4", "msg-5"))
  }

  test("batches from different streams are processed concurrently") {
    val received = new LinkedBlockingQueue[String]()
    val queue = makeQueue(batch => batch.foreach(log => received.put(log.message)))

    queue.sendBatch(List(sampleLog("stream-a-1"), sampleLog("stream-a-2")))
    queue.sendBatch(List(sampleLog("stream-b-1"), sampleLog("stream-b-2")))

    val messages = (1 to 4).map(_ => received.poll(10, TimeUnit.SECONDS)).toSet
    assertEquals(messages, Set("stream-a-1", "stream-a-2", "stream-b-1", "stream-b-2"))
  }

  test("sent counter increments by batch size on success") {
    val registry = new DefaultRegistry()
    val received = new LinkedBlockingQueue[Seq[OtelLog]]()
    val queue = makeQueue(batch => received.put(batch), registry)

    queue.sendBatch(List(sampleLog("a"), sampleLog("b"), sampleLog("c")))
    received.poll(10, TimeUnit.SECONDS)
    Thread.sleep(200)

    assertEquals(registry.counter("atlas.cloudwatch.logs.queue.sent").count(), 3L)
  }

  test("sendBatch exception drops whole batch and increments dropped by batch size") {
    val registry = new DefaultRegistry()
    val received = new LinkedBlockingQueue[String]()

    val queue = makeQueue(
      batch => {
        // Fail deterministically based on content, not call order — avoids race with parallelism>1
        if (batch.exists(_.message.startsWith("will-fail")))
          throw new RuntimeException("TCP error")
        else
          batch.foreach(log => received.put(log.message))
      },
      registry
    )

    queue.sendBatch(List(sampleLog("will-fail"), sampleLog("also-dropped")))
    queue.sendBatch(List(sampleLog("ok-1"), sampleLog("ok-2")))

    assertEquals(received.poll(10, TimeUnit.SECONDS), "ok-1")
    assertEquals(received.poll(10, TimeUnit.SECONDS), "ok-2")

    Thread.sleep(200)
    assertEquals(registry.counter("atlas.cloudwatch.logs.queue.dropped").count(), 2L)
    assertEquals(registry.counter("atlas.cloudwatch.logs.queue.sent").count(), 2L)
  }

  test("sendBatch does not throw when queue is full") {
    val queue = makeQueue(
      _ => Thread.sleep(5000),
      queueSize = 1
    )

    (1 to 20).foreach(_ => queue.sendBatch(List(sampleLog())))
  }
}
