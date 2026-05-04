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

import com.netflix.atlas.pekko.StreamOps
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * Bounded async queue that drains log batches to OtelTcpLogger.sendBatch.
 *
 * Each element in the queue is a batch of OtelLog entries from a single log stream.
 * Up to `parallelism` batches are processed concurrently (one per stream), with each
 * batch sent over a single TCP connection — one socket open/write/close per batch
 * instead of one per log. This eliminates per-log goroutine pressure on the OTel collector.
 *
 * Absorbs traffic bursts: callers never block on TCP I/O. When the queue is full the
 * batch is dropped (counted) rather than causing OOM in the OTel collector.
 *
 * TCP sends run on a dedicated thread pool (logs-sink-dispatcher) so blocking calls
 * and retry sleeps never starve the main Akka dispatcher.
 */
class LogsPublishQueue(
  config: Config,
  registry: Registry,
  tcpSendBatch: Seq[OtelLog] => Unit
)(implicit system: ActorSystem)
    extends OtelLogSink
    with StrictLogging {

  // Futures completed on the main dispatcher (stream control path).
  private implicit val ec: ExecutionContext = system.dispatcher

  // Blocking TCP work runs here, isolated from the main dispatcher.
  private val sinkDispatcher: ExecutionContext =
    system.dispatchers.lookup("logs-sink-dispatcher")

  private val queueSize = config.getInt("atlas.cloudwatch.logs.queue.queueSize")
  private val parallelism = config.getInt("atlas.cloudwatch.logs.queue.parallelism")

  private val logsSent = registry.counter("atlas.cloudwatch.logs.queue.sent")
  private val logsDropped = registry.counter("atlas.cloudwatch.logs.queue.dropped")

  private val queue = StreamOps
    .blockingQueue[Seq[OtelLog]](registry, "logsQueue", queueSize)
    .mapAsync(parallelism) { batch =>
      Future {
        // One TCP connection for the entire batch — all logs written then socket closed.
        tcpSendBatch(batch)
        logsSent.increment(batch.size)
      }(sinkDispatcher).recover {
        case e: Exception =>
          logger.warn(s"Failed to send log batch to OTel collector: ${e.getMessage}", e)
          logsDropped.increment(batch.size)
      }
    }
    .toMat(Sink.ignore)(Keep.left)
    .run()

  override def sendBatch(logs: Seq[OtelLog]): Unit = queue.offer(logs)
}
