/*
 * Copyright 2014-2024 Netflix, Inc.
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

import com.netflix.atlas.cloudwatch.poller.DoubleValue
import com.netflix.atlas.cloudwatch.poller.PublishClient
import com.netflix.atlas.cloudwatch.poller.PublishConfig
import com.netflix.atlas.core.model.Datapoint
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.pekko.CustomMediaTypes
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.json.Json
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Simple queue for batching data points to be sent to publish proxy. Retries occur only on 429s, 504s or exceptions
  * from the client.
  */
class PublishQueue(
  config: Config,
  registry: Registry,
  val stack: String,
  val uri: String,
  val configUri: String,
  val evalUri: String,
  val status: LeaderStatus,
  httpClient: PekkoHttpClient,
  scheduler: ScheduledExecutorService
)(implicit system: ActorSystem)
    extends StrictLogging {

  private implicit val executionContext: ExecutionContext = system.dispatcher

  private val datapointsDropped =
    registry.createId("atlas.cloudwatch.queue.dps.dropped", "stack", stack)
  private val datapointsSent = registry.counter("atlas.cloudwatch.queue.dps.sent", "stack", stack)
  private val retryAttempts = registry.counter("atlas.cloudwatch.queue.retries", "stack", stack)
  private val droppedRetries = registry.counter(datapointsDropped.withTags("reason", "maxRetries"))

  private val registrySent =
    registry.createId("atlas.cloudwatch.queue.registry.sent", "stack", stack)

  private val maxRetries = getSetting("maxRetries")
  private val queueSize = getSetting("queueSize")
  private val batchSize = getSetting("batchSize")
  private val batchTimeout = getDurationSetting("batchTimeout")

  private val registryPublishClient = new PublishClient(
    new PublishConfig(config, uri, configUri, evalUri, status = status, registry = registry)
  )

  private val lastUpdateTimestamp =
    PolledMeter
      .using(registry)
      .withId(registry.createId("atlas.cloudwatch.queue.lastPublish", "stack", stack))
      .monitorValue(
        new AtomicLong(System.currentTimeMillis()),
        Functions.AGE
      )

  private[cloudwatch] val publishQueue = StreamOps
    .blockingQueue[AtlasDatapoint](registry, s"${stack}PubQueue", queueSize)
    .groupedWithin(batchSize, FiniteDuration(batchTimeout.toNanos, TimeUnit.NANOSECONDS))
    .map(publish)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  logger.info(
    s"Setup queue for stack ${stack} publishing URI ${uri}, lwc-config URI ${configUri}, eval URI ${evalUri}"
  )

  def updateRegistry(dp: AtlasDatapoint, cwDatapoint: CloudWatchDatapoint): Unit = {
    val atlasDp = toDoubleValue(dp)
    registryPublishClient.updateCounter(atlasDp.id, atlasDp.value)
    if (dp.dsType == DsType.Rate) {
      registry
        .counter(
          updateSelfMetrics(dp, cwDatapoint)
        )
        .increment()
    } else if (dp.dsType == DsType.Gauge) {
      registryPublishClient.updateGauge(atlasDp.id, atlasDp.value)
      registry
        .counter(
          updateSelfMetrics(dp, cwDatapoint)
        )
        .increment()
    } else {
      logger.error(s"Unknown ds type, skip updating registry ${dp.dsType}")
    }
  }

  private def updateSelfMetrics(dp: AtlasDatapoint, cwDatapoint: CloudWatchDatapoint) = {
    registrySent.withTags(
      "type",
      dp.dsType.toString,
      "unit",
      cwDatapoint.unitAsString(),
      "stats",
      dp.tags.getOrElse("statistic", "NO_STATS"),
      "metric",
      dp.tags("name")
    )
  }

  private def toDoubleValue(
    datapoint: AtlasDatapoint
  ): DoubleValue = {
    DoubleValue(datapoint.tags, datapoint.value)
  }

  def enqueue(datapoint: AtlasDatapoint): Unit = publishQueue.offer(datapoint)

  private[cloudwatch] def publish(datapoints: Seq[AtlasDatapoint]): Future[NotUsed] = {
    val size = datapoints.size
    val payload = Json.smileEncode(MetricsPayload(Map.empty, datapoints))
    datapointsSent.increment(size)
    publish(payload, 0, size)
  }

  private[cloudwatch] def publish(
    payload: Array[Byte],
    retries: Int,
    size: Int
  ): Future[NotUsed] = {
    val request = HttpRequest(
      HttpMethods.POST,
      uri = uri,
      entity = HttpEntity(
        CustomMediaTypes.`application/x-jackson-smile`,
        ByteString.fromArrayUnsafe(payload)
      )
    )
    val promise = Promise[NotUsed]()
    httpClient.singleRequest(request).onComplete {
      case Success(response) =>
        response.status.intValue() match {
          case 200 => // All is well
            response.discardEntityBytes()
            promise.complete(Try(NotUsed))
          case 202 | 206 => // Partial failure
            val id = datapointsDropped.withTag("reason", "partialFailure")
            incrementFailureCount(id, response, size, promise)
          case 400 => // Bad message, all data dropped
            val id = datapointsDropped.withTag("reason", "completeFailure")
            incrementFailureCount(id, response, size, promise)
          case 429 => // backoff
            retry(payload, retries, size)
            response.discardEntityBytes()
            promise.complete(Try(NotUsed))
          case v => // Unexpected, assume all dropped
            val id = datapointsDropped.withTag("reason", s"status_$v")
            registry.counter(id).increment(size)
            response.discardEntityBytes()
            promise.complete(Try(NotUsed))
        }

      case Failure(ex) =>
        logger.error(s"Failed publishing to ${uri} for ${stack}", ex)
        retry(payload, retries, size)
        promise.complete(Try(NotUsed))
    }
    lastUpdateTimestamp.set(System.currentTimeMillis())
    promise.future
  }

  private def incrementFailureCount(
    id: Id,
    response: HttpResponse,
    size: Int,
    promise: Promise[NotUsed]
  ): Unit = {
    response.entity.dataBytes.runReduce(_ ++ _).onComplete {
      case Success(bs) =>
        try {
          val msg = Json.decode[FailureResponse](bs.toArray)
          msg.message.headOption.foreach { reason =>
            logger.warn("failed to validate some datapoints, first reason: {}", reason)
          }
          registry.counter(id).increment(msg.errorCount)
          promise.complete(Try(NotUsed))
        } catch {
          case ex: Throwable =>
            logger.warn("Failed to pub proxy response", ex)
            promise.complete(Try(NotUsed))
        }
      case Failure(_) =>
        registry.counter(id).increment(size)
        promise.complete(Try(NotUsed))
    }
  }

  private def getSetting(setting: String): Int = {
    config.hasPath(s"queue.${stack}.${setting}") match {
      case false => config.getInt(s"queue.${setting}")
      case true  => config.getInt(s"queue.${stack}.${setting}")
    }
  }

  private def getDurationSetting(setting: String): Duration = {
    config.hasPath(s"queue.${stack}.${setting}") match {
      case false => config.getDuration(s"queue.${setting}")
      case true  => config.getDuration(s"queue.${stack}.${setting}")
    }
  }

  private def retry(payload: Array[Byte], retries: Int, size: Int): Unit = {
    if (retries >= maxRetries) {
      droppedRetries.increment(size)
      return
    }

    val numRetries = retries + 1
    val delay: Long = 50 + (1 << numRetries) // exponential backoff starting at 52ms
    val run: Runnable = () => publish(payload, numRetries, size)
    scheduler.schedule(run, delay, TimeUnit.MILLISECONDS)
    retryAttempts.increment()
  }
}

/**
  * Represents a failure response message from the publish endpoint.
  *
  * @param `type`
  * Message type. Should always be "error".
  * @param errorCount
  * Number of datapoints that failed validation.
  * @param message
  * Reasons for why datapoints were dropped.
  */
case class FailureResponse(`type`: String, errorCount: Int, message: List[String])

/**
  * Metrics payload that pollers will send back to the manager.
  *
  * @param tags
  * Common tags that should get added to all metrics in the payload.
  * @param metrics
  * Metrics collected by the poller.
  */
case class MetricsPayload(
  tags: Map[String, String] = Map.empty,
  metrics: Iterable[Datapoint] = Nil
)
