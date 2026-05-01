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

import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/**
 * Queue + single worker that sends OTEL logs over TCP using OtelTcpLogger.
 *
 * - send(log) enqueues; if the queue is full, the log is dropped.
 * - A single background worker thread drains the queue and calls OtelTcpLogger.sendLog.
 */
class OtelTcpQueueSink(
  queueCapacity: Int,
  workerName: String = "otel-tcp-queue-sink-worker"
) extends OtelLogSink
    with StrictLogging {

  private val queue: BlockingQueue[OtelLog] =
    new ArrayBlockingQueue[OtelLog](queueCapacity)

  private val executor = Executors.newSingleThreadExecutor(new ThreadFactory {

    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, workerName)
      t.setDaemon(true)
      t
    }
  })

  @volatile private var running = true

  // Start worker
  executor.submit(new Runnable {

    override def run(): Unit = {
      logger.info(s"Starting OTEL TCP queue sink worker: $workerName, capacity=$queueCapacity")
      while (running) {
        try {
          // Block until a log is available
          val log = queue.take()
          OtelTcpLogger.sendLog(log)
        } catch {
          case _: InterruptedException =>
            running = false
          case NonFatal(e) =>
            // Log and continue; don't kill the worker
            logger.warn(s"OTEL TCP queue sink worker error: ${e.getMessage}", e)
        }
      }
      logger.info(s"Stopping OTEL TCP queue sink worker: $workerName")
    }
  })

  /** Enqueue a log for sending. Drops the log if the queue is full. */
  override def send(log: OtelLog): Unit = {
    val offered = queue.offer(log)
    if (!offered) {
      // Optional: add a Spectator counter for dropped logs
      logger.debug("OTEL TCP queue full, dropping log")
    }
  }

  /** Optional graceful shutdown hook. */
  def shutdown(): Unit = {
    running = false
    executor.shutdownNow()
    executor.awaitTermination(5, TimeUnit.SECONDS)
  }
}
