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
import com.netflix.atlas.pekko.StreamOps.SourceQueue
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function as JFunction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

/**
 * Bounded async queue that drains log batches to OtelTcpLogger.sendBatch.
 *
 * Each element in a queue is a batch of OtelLog entries from a single log stream.
 * A dedicated queue is created per (source AWS account, log group) pair, so a single
 * heavy-hitter log group can only fill and drop batches from its own queue — it cannot
 * starve or drop traffic for other log groups, even ones in the same account, sharing
 * this process. Within a queue, up to `parallelism` batches are processed concurrently
 * (one per stream), with each batch sent over a single TCP connection — one socket
 * open/write/close per batch instead of one per log. This eliminates per-log goroutine
 * pressure on the OTel collector.
 *
 * Per-key queues alone don't bound total memory: with enough distinct (account, logGroup)
 * pairs, `keys * queueSize` batches could be queued at once. A shared in-flight counter
 * caps the total across all keys at `maxTotalInFlight`, so the aggregate memory footprint
 * (and thus the OTel collector's exposure) stays bounded regardless of how many keys are
 * active; `queueSize` remains a secondary per-key bound so no single log group can consume
 * the entire global budget on its own.
 *
 * Known heavy-hitter accounts/log groups can be capped further via `perKeyQueueSize`,
 * keyed by `"account:logGroup"`, which overrides `queueSize` for specific keys without
 * lowering the default for everyone else.
 *
 * Absorbs traffic bursts: callers never block on TCP I/O. When a queue (or the global
 * cap) is full the batch is dropped (counted) rather than causing OOM in the OTel
 * collector.
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
  private val maxTotalInFlight = config.getInt("atlas.cloudwatch.logs.queue.maxTotalInFlight")

  // Per-key queueSize overrides, for known heavy-hitter account/logGroup pairs that
  // would otherwise be allowed to buffer up to the default queueSize on their own.
  // Keyed by "account:logGroup".
  private val perKeyQueueSizePath = "atlas.cloudwatch.logs.queue.perKeyQueueSize"

  private val perKeyQueueSize: Map[String, Int] =
    if (config.hasPath(perKeyQueueSizePath))
      config
        .getObject(perKeyQueueSizePath)
        .unwrapped()
        .asScala
        .view
        .mapValues(_.asInstanceOf[Number].intValue())
        .toMap
    else Map.empty

  private val logsSent = registry.counter("atlas.cloudwatch.logs.queue.sent")
  private val logsDropped = registry.counter("atlas.cloudwatch.logs.queue.dropped")

  // Number of TCP connections opened to the OTel collector — one per sendBatch call,
  // regardless of how many logs are in the batch. Tracks the connection/goroutine
  // churn rate the collector actually experiences, which drives its own memory use
  // independently of whether we ever drop or queue anything on our side.
  private val batchesSent = registry.counter("atlas.cloudwatch.logs.queue.batchesSent")

  private val logsDroppedGlobalCap =
    registry.counter("atlas.cloudwatch.logs.queue.droppedGlobalCap")

  // "account:logGroup" -> dedicated queue, created lazily on first batch seen for that key.
  private val keyQueues = new ConcurrentHashMap[String, SourceQueue[Seq[OtelLog]]]()

  // Batches queued or currently being sent, across every key's queue.
  private val totalInFlight = new AtomicInteger(0)

  private val createQueue: JFunction[String, SourceQueue[Seq[OtelLog]]] = key =>
    StreamOps
      .blockingQueue[Seq[OtelLog]](
        registry,
        s"logsQueue-$key",
        perKeyQueueSize.getOrElse(key, queueSize)
      )
      .mapAsync(parallelism) { batch =>
        Future {
          // One TCP connection for the entire batch — all logs written then socket closed.
          tcpSendBatch(batch)
          batchesSent.increment()
          logsSent.increment(batch.size)
        }(sinkDispatcher)
          .recover {
            case e: Exception =>
              logger.warn(s"Failed to send log batch to OTel collector: ${e.getMessage}", e)
              logsDropped.increment(batch.size)
          }
          .andThen { case _ => totalInFlight.decrementAndGet() }
      }
      .toMat(Sink.ignore)(Keep.left)
      .run()

  override def sendBatch(account: String, logGroup: String, logs: Seq[OtelLog]): Unit = {
    val key = s"$account:$logGroup"
    if (totalInFlight.incrementAndGet() > maxTotalInFlight) {
      totalInFlight.decrementAndGet()
      logsDroppedGlobalCap.increment(logs.size)
    } else if (!keyQueues.computeIfAbsent(key, createQueue).offer(logs)) {
      // Rejected by the per-key queue (already full); release the slot we reserved.
      totalInFlight.decrementAndGet()
    }
  }
}
