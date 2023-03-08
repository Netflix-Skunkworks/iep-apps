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
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.netflix.atlas.akka.AkkaHttpClient
import com.netflix.atlas.akka.CustomMediaTypes
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.json.Json
import com.netflix.atlas.poller.Messages
import com.netflix.atlas.poller.Messages.MetricsPayload
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
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
  httpClient: AkkaHttpClient,
  scheduler: ScheduledExecutorService
)(implicit system: ActorSystem)
    extends StrictLogging {

  private implicit val executionContext = system.dispatcher

  private val datapointsDropped =
    registry.createId("atlas.cloudwatch.queue.dps.dropped", "stack", stack)
  private val datapointsSent = registry.counter("atlas.cloudwatch.queue.dps.sent", "stack", stack)
  private val retryAttempts = registry.counter("atlas.cloudwatch.queue.retries", "stack", stack)
  private val droppedRetries = registry.counter(datapointsDropped.withTags("reason", "maxRetries"))

  private val maxRetries = getSetting("maxRetries")
  private val queueSize = getSetting("queueSize")
  private val batchSize = getSetting("batchSize")
  private val batchTimeout = getDurationSetting("batchTimeout")

  private[cloudwatch] val publishQueue = StreamOps
    .blockingQueue[AtlasDatapoint](registry, s"${stack}PubQueue", queueSize)
    .groupedWithin(batchSize, FiniteDuration(batchTimeout.toNanos, TimeUnit.NANOSECONDS))
    .map(publish)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  /**
    * Adds the data point to the queue if there is room. Increments a metric if not.
    *
    * @param datapoint
    *     The non-null data point to enqueue.
    */
  def enqueue(datapoint: AtlasDatapoint): Unit = publishQueue.offer(datapoint)

  private[cloudwatch] def publish(datapoints: Seq[AtlasDatapoint]): Future[Unit] = {
    val size = datapoints.size
    val payload = Json.smileEncode(MetricsPayload(Map.empty, datapoints))
    datapointsSent.increment(size)
    publish(payload, 0, size)
  }

  private[cloudwatch] def publish(payload: Array[Byte], retries: Int, size: Int): Future[Unit] = {
    val request = HttpRequest(
      HttpMethods.POST,
      uri = uri,
      entity = HttpEntity(
        CustomMediaTypes.`application/x-jackson-smile`,
        ByteString.fromArrayUnsafe(payload)
      )
    )
    val promise = Promise[Unit]()
    httpClient.singleRequest(request).onComplete {
      case Success(response) =>
        response.status.intValue() match {
          case 200 => // All is well
            response.discardEntityBytes()
            promise.complete(Try(null))
          case 202 | 206 => // Partial failure
            val id = datapointsDropped.withTag("reason", "partialFailure")
            incrementFailureCount(id, response, size, promise)
          case 400 => // Bad message, all data dropped
            val id = datapointsDropped.withTag("reason", "completeFailure")
            incrementFailureCount(id, response, size, promise)
          case 429 => // backoff
            retry(payload, retries, size)
            response.discardEntityBytes()
            promise.complete(Try(null))
          case v => // Unexpected, assume all dropped
            val id = datapointsDropped.withTag("reason", s"status_$v")
            registry.counter(id).increment(size)
            response.discardEntityBytes()
            promise.complete(Try(null))
        }

      case Failure(ex) =>
        logger.error(s"Failed publishing to ${uri} for ${stack}", ex)
        retry(payload, retries, size)
        promise.complete(Try(null))
    }
    promise.future
  }

  private def incrementFailureCount(
    id: Id,
    response: HttpResponse,
    size: Int,
    promise: Promise[Unit]
  ): Unit = {
    response.entity.dataBytes.runReduce(_ ++ _).onComplete {
      case Success(bs) =>
        try {
          val msg = Json.decode[Messages.FailureResponse](bs.toArray)
          msg.message.headOption.foreach { reason =>
            logger.warn("failed to validate some datapoints, first reason: {}", reason)
          }
          registry.counter(id).increment(msg.errorCount)
          promise.complete(Try(null))
        } catch {
          case ex: Throwable =>
            logger.warn("Failed to pub proxy response", ex)
            promise.complete(Try(null))
        }
      case Failure(_) =>
        registry.counter(id).increment(size)
        promise.complete(Try(null))
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
