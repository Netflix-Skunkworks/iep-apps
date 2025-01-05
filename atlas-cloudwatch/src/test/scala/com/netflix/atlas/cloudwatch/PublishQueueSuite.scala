/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.javadsl.model.ContentType
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.ResponseEntity
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.testkit.TestKitBase
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.pekko.CustomMediaTypes
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json.Json
import com.netflix.atlas.webapi.PublishApi.FailureMessage
import com.netflix.iep.leader.api.LeaderStatus
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
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class PublishQueueSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  var registry: Registry = null
  var httpClient = mock[PekkoHttpClient]
  var httpCaptor = ArgCaptor[HttpRequest]
  var scheduler = mock[ScheduledExecutorService]
  val timestamp = 1672531200000L
  val config = ConfigFactory.load().getConfig("atlas.cloudwatch.account.routing")
  val threadSleep = 300
  var leaderStatus: LeaderStatus = null

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    httpClient = mock[PekkoHttpClient]
    httpCaptor = ArgCaptor[HttpRequest]
    scheduler = mock[ScheduledExecutorService]
    leaderStatus = mock[LeaderStatus]

    when(leaderStatus.hasLeadership).thenReturn(true)
  }

  test("publish success") {
    mockResponse(StatusCodes.OK)
    val queue =
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    Await.ready(
      queue.publish(
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      ),
      60.seconds
    )

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
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    Await.ready(
      queue.publish(
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      ),
      60.seconds
    )

    assertCounters(2, dropped = Map("partialFailure" -> 1))
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 400") {
    mockResponse(StatusCodes.BadRequest, Json.encode(FailureMessage("foo", 2, List("Err"))))
    val queue =
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    Await.ready(
      queue.publish(
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      ),
      60.seconds
    )

    assertCounters(2, dropped = Map("completeFailure" -> 2))
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 500") {
    mockResponse(StatusCodes.InternalServerError)
    val queue =
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    Await.ready(
      queue.publish(
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      ),
      60.seconds
    )

    assertCounters(2, dropped = Map("status_500" -> 2))
    verify(httpClient, times(1)).singleRequest(httpCaptor)
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.value.entity.getContentType()
    )
  }

  test("publish 429") {
    mockResponse(StatusCodes.TooManyRequests)
    val queue =
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    Await.ready(
      queue.publish(
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      ),
      60.seconds
    )

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
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    val payload = Json.smileEncode(
      MetricsPayload(
        Map.empty,
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      )
    )
    Await.ready(queue.publish(payload, 1, 2), 60.seconds)

    assertCounters(dropped = Map("maxRetries" -> 2))
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
      new PublishQueue(
        config,
        registry,
        "main",
        "http://localhost",
        "http://localhost",
        "http://localhost",
        leaderStatus,
        httpClient,
        scheduler
      )
    Await.ready(
      queue.publish(
        Seq(
          Datapoint(Map("k1" -> "v1"), timestamp, 42.5),
          Datapoint(Map("k2" -> "v2"), timestamp, 24.1)
        )
      ),
      60.seconds
    )

    assertCounters(2, retries = 1)
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
    dropped: Map[String, Long] = Map.empty
  ): Unit = {
    assertEquals(registry.counter("atlas.cloudwatch.queue.dps.sent", "stack", "main").count, sent)
    assertEquals(registry.counter("atlas.cloudwatch.queue.retries", "stack", "main").count, retries)
    List("partialFailure", "completeFailure", "status_500", "maxRetries").foreach { reason =>
      assertEquals(
        registry
          .counter("atlas.cloudwatch.queue.dps.dropped", "stack", "main", "reason", reason)
          .count,
        dropped.getOrElse(reason, 0L),
        s"Count differs for ${reason}"
      )
    }
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
