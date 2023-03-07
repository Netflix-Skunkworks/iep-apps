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
import akka.http.javadsl.model.ContentType
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.ResponseEntity
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKitBase
import com.netflix.atlas.akka.AkkaHttpClient
import com.netflix.atlas.akka.CustomMediaTypes
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json.Json
import com.netflix.atlas.poller.Messages.MetricsPayload
import com.netflix.atlas.webapi.PublishApi.FailureMessage
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.ArgumentMatchersSugar.anyLong
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.never
import org.mockito.MockitoSugar.times
import org.mockito.MockitoSugar.verify
import org.mockito.MockitoSugar.when
import org.mockito.captor.ArgCaptor

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

class PublishQueueSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  var registry: Registry = null
  var httpClient = mock[AkkaHttpClient]
  var httpCaptor = ArgCaptor[HttpRequest]
  var scheduler = mock[ScheduledExecutorService]
  val timestamp = 1672531200000L
  val config = ConfigFactory.load().getConfig("atlas.cloudwatch.account.routing")
  val threadSleep = 300

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    httpClient = mock[AkkaHttpClient]
    httpCaptor = ArgCaptor[HttpRequest]
    scheduler = mock[ScheduledExecutorService]
  }

  test("publish success") {
    mockResponse(StatusCodes.OK)
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    queue.publish(
      Seq(
        Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
        Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
      )
    )

    Thread.sleep(threadSleep)
    assertCounters(2)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 202") {
    mockResponse(StatusCodes.Accepted, Json.encode(FailureMessage("foo", 1, List("Err"))))
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    queue.publish(
      Seq(
        Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
        Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
      )
    )

    Thread.sleep(threadSleep)
    assertCounters(2, droppedPartial = 1)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 400") {
    mockResponse(StatusCodes.BadRequest, Json.encode(FailureMessage("foo", 2, List("Err"))))
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    queue.publish(
      Seq(
        Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
        Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
      )
    )

    Thread.sleep(threadSleep)
    assertCounters(2, droppedComplete = 2)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 500") {
    mockResponse(StatusCodes.InternalServerError)
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    queue.publish(
      Seq(
        Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
        Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
      )
    )

    Thread.sleep(threadSleep)
    assertCounters(2, dropped500 = 2)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 429") {
    mockResponse(StatusCodes.TooManyRequests)
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    queue.publish(
      Seq(
        Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
        Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
      )
    )

    Thread.sleep(threadSleep)
    assertCounters(2, retries = 1)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
    verify(scheduler, times(1)).schedule(any[Runnable], anyLong, any[TimeUnit])
  }

  test("publish 429 too many attempts") {
    mockResponse(StatusCodes.TooManyRequests)
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    val payload = Json.smileEncode(
      MetricsPayload(
        Map.empty,
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      )
    )
    queue.publish(payload, 1, 2)

    Thread.sleep(threadSleep)
    assertCounters(droppedRetries = 2)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
    verify(scheduler, never).schedule(any[Runnable], anyLong, any[TimeUnit])
  }

  test("publish exception") {
    mockResponse(StatusCodes.OK, t = new UTException("UT"))
    val queue =
      new PublishQueue(config, registry, "main", "http://localhost", httpClient, scheduler)
    queue.publish(
      Seq(
        Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
        Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
      )
    )

    Thread.sleep(threadSleep)
    assertCounters(2, exceptions = 1, retries = 1)
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
    verify(scheduler, times(1)).schedule(any[Runnable], anyLong, any[TimeUnit])
  }

  def assertCounters(
    sent: Long = 0,
    retries: Long = 0,
    droppedPartial: Long = 0,
    droppedComplete: Long = 0,
    dropped500: Long = 0,
    droppedQueueFull: Long = 0,
    droppedRetries: Long = 0,
    exceptions: Long = 0
  ): Unit = {
    assertEquals(registry.counter("atlas.cloudwatch.queue.dps.sent", "stack", "main").count, sent)
    assertEquals(registry.counter("atlas.cloudwatch.queue.retries", "stack", "main").count, retries)
    assertEquals(
      registry
        .counter("atlas.cloudwatch.queue.dps.dropped", "stack", "main", "reason", "partialFailure")
        .count,
      droppedPartial
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.queue.dps.dropped", "stack", "main", "reason", "completeFailure")
        .count,
      droppedComplete
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.queue.dps.dropped", "stack", "main", "reason", "status_500")
        .count,
      dropped500
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.queue.dps.dropped", "stack", "main", "reason", "queueFull")
        .count,
      droppedQueueFull
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.queue.dps.dropped", "stack", "main", "reason", "maxRetries")
        .count,
      droppedRetries
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.queue.exception", "stack", "main", "ex", "UTException")
        .count,
      exceptions
    )
  }

  def mockResponse(statusCode: StatusCode, content: String = null, t: Throwable = null): Unit = {
    val httpEntity: ResponseEntity = if (content != null) {
      HttpEntity(ContentTypes.`application/json`, content)
    } else HttpEntity.Empty
    if (t == null)
      when(httpClient.singleRequest(any[HttpRequest])).thenAnswer(
        Future.successful(
          HttpResponse(
            statusCode,
            entity = httpEntity
          )
        )
      )
    else {
      val called = new AtomicBoolean()
      when(httpClient.singleRequest(any[HttpRequest]))
        .thenAnswer(
          if (called.get()) {
            Future.successful(
              HttpResponse(
                statusCode,
                entity = httpEntity
              )
            )
          } else {
            called.set(true)
            Future.failed(t)
          }
        )
    }
  }

  class UTException(msg: String) extends RuntimeException(msg)
}