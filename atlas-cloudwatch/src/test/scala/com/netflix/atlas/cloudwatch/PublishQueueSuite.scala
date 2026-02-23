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

import com.netflix.atlas.cloudwatch.poller.PublishClient
import com.netflix.atlas.cloudwatch.poller.PublishConfig
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
import com.netflix.atlas.json3.Json
import com.netflix.atlas.webapi.PublishApi.FailureMessage
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.ArgumentCaptor
import org.mockito.stubbing.Answer

import java.time.Instant
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class PublishQueueSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private var registry: Registry = _
  private var httpClient = mock(classOf[PekkoHttpClient])
  private var httpCaptor = ArgumentCaptor.forClass(classOf[HttpRequest])
  private var publishClient = mock(classOf[PublishClient])
  private var secondaryPublishClient = mock(classOf[PublishClient])
  private var scheduler = mock(classOf[ScheduledExecutorService])
  private val timestamp = 1672531200000L
  private val config = ConfigFactory.load().getConfig("atlas.cloudwatch.account.routing")
  private var leaderStatus: LeaderStatus = _

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    httpClient = mock(classOf[PekkoHttpClient])
    httpCaptor = ArgumentCaptor.forClass(classOf[HttpRequest])
    publishClient = mock(classOf[PublishClient])
    secondaryPublishClient = mock(classOf[PublishClient])
    scheduler = mock(classOf[ScheduledExecutorService])
    leaderStatus = mock(classOf[LeaderStatus])

    when(leaderStatus.hasLeadership).thenReturn(true)
  }

  test("publish success") {
    mockResponse(StatusCodes.OK)
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
  }

  test("publish 202") {
    mockResponse(StatusCodes.Accepted, Json.encode(FailureMessage("foo", 1, List("Err"))))
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
  }

  test("publish 400") {
    mockResponse(StatusCodes.BadRequest, Json.encode(FailureMessage("foo", 2, List("Err"))))
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
  }

  test("publish 500") {
    mockResponse(StatusCodes.InternalServerError)
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
  }

  test("publish 429") {
    mockResponse(StatusCodes.TooManyRequests)
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
    verify(scheduler, times(1)).schedule(any[Runnable], anyLong, any[TimeUnit])
  }

  test("publish 429 too many attempts") {
    mockResponse(StatusCodes.TooManyRequests)
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
    verify(scheduler, never).schedule(any[Runnable], anyLong, any[TimeUnit])
  }

  test("publish exception") {
    mockResponse(StatusCodes.OK, t = new UTException("UT"))
    val queue = getQueue
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
    verify(httpClient, times(1)).singleRequest(httpCaptor.capture())
    assertEquals(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType.asInstanceOf[ContentType],
      httpCaptor.getValue.entity.getContentType()
    )
    verify(scheduler, times(1)).schedule(any[Runnable], anyLong, any[TimeUnit])
  }

  test("updateRegistry rate 5s primary only") {
    mockResponse(StatusCodes.OK)
    val dp = new AtlasDatapoint(Map("name" -> "metric.foo"), Instant.now().toEpochMilli, 1, 5_000)
    val queue = getQueue
    queue.updateRegistry(dp, mockAZDP())
    verify(publishClient, times(1)).updateCounter(Id.create("metric.foo"), 5)
    verify(secondaryPublishClient, never()).updateCounter(any[Id], any[Double])
  }

  test("updateRegistry rate 60s primary only") {
    mockResponse(StatusCodes.OK)
    val dp = new AtlasDatapoint(Map("name" -> "metric.foo"), Instant.now().toEpochMilli, 1, 60_000)
    val queue = getQueue
    queue.updateRegistry(dp, mockAZDP())
    verify(publishClient, times(1)).updateCounter(Id.create("metric.foo"), 60)
    verify(secondaryPublishClient, never()).updateCounter(any[Id], any[Double])
  }

  test("updateRegistry dual-registry rate updates both registries") {
    mockResponse(StatusCodes.OK)
    val dp = new AtlasDatapoint(Map("name" -> "metric.dual"), Instant.now().toEpochMilli, 2, 5_000)
    val queue = getDualQueue
    queue.updateRegistry(dp, mockAZDP())

    // 2 * (step/1000) = 2 * 5 = 10
    verify(publishClient, times(1))
      .updateCounter(
        argThat[Id](id => id.name() == "metric.dual"),
        eqTo(10.0d)
      )

    verify(secondaryPublishClient, times(1))
      .updateCounter(
        argThat[Id](id => id.name() == "metric.dual"),
        eqTo(10.0d)
      )
  }

  test("updateRegistry dual-registry gauge updates both registries") {
    mockResponse(StatusCodes.OK)
    val dp = new AtlasDatapoint(
      Map("name" -> "metric.gauge", "atlas.dstype" -> "gauge"),
      Instant.now().toEpochMilli,
      3.14,
      5_000
    )
    val queue = getDualQueue
    queue.updateRegistry(dp, mockAZDP())

    verify(publishClient, times(1))
      .updateGauge(
        argThat[Id](id => id.name() == "metric.gauge"),
        eqTo(3.14d)
      )

    verify(secondaryPublishClient, times(1))
      .updateGauge(
        argThat[Id](id => id.name() == "metric.gauge"),
        eqTo(3.14d)
      )
  }

  test("updateRegistry dual-registry disabled does not call secondary") {
    mockResponse(StatusCodes.OK)
    val dp =
      new AtlasDatapoint(Map("name" -> "metric.single"), Instant.now().toEpochMilli, 1, 5_000)
    // dualRegistryEnabled = false even though secondaryClientOpt is defined
    val queue = getQueueWithSecondaryEnabledFlag(false)
    queue.updateRegistry(dp, mockAZDP())

    verify(publishClient, times(1))
      .updateCounter(
        argThat[Id](id => id.name() == "metric.single"),
        eqTo(5.0d)
      )
    verify(secondaryPublishClient, never()).updateCounter(any[Id], any[Double])
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
        s"Count differs for $reason"
      )
    }
  }

  /** Queue with primary only (no secondary, dualRegistryEnabled = false) */
  def getQueue: PublishQueue = {
    val pubClientConf = new PublishConfig(
      config,
      null,
      null,
      null,
      leaderStatus,
      registry
    )
    when(publishClient.config).thenReturn(pubClientConf)

    new PublishQueue(
      config,
      registry,
      "main",
      leaderStatus,
      publishClient,
      None,
      dualRegistryEnabled = false,
      httpClient,
      scheduler
    )
  }

  /** Queue with both primary and secondary, dualRegistryEnabled = true */
  def getDualQueue: PublishQueue = {
    val pubClientConf = new PublishConfig(
      config,
      null,
      null,
      null,
      leaderStatus,
      registry
    )
    when(publishClient.config).thenReturn(pubClientConf)
    when(secondaryPublishClient.config).thenReturn(pubClientConf)

    new PublishQueue(
      config,
      registry,
      "main",
      leaderStatus,
      publishClient,
      Some(secondaryPublishClient),
      dualRegistryEnabled = true,
      httpClient,
      scheduler
    )
  }

  /** Helper to build a queue with a secondary client but configurable dualRegistryEnabled flag */
  def getQueueWithSecondaryEnabledFlag(dualEnabled: Boolean): PublishQueue = {
    val pubClientConf = new PublishConfig(
      config,
      null,
      null,
      null,
      leaderStatus,
      registry
    )
    when(publishClient.config).thenReturn(pubClientConf)
    when(secondaryPublishClient.config).thenReturn(pubClientConf)

    new PublishQueue(
      config,
      registry,
      "main",
      leaderStatus,
      publishClient,
      Some(secondaryPublishClient),
      dualRegistryEnabled = dualEnabled,
      httpClient,
      scheduler
    )
  }

  def mockAZDP(): software.amazon.awssdk.services.cloudwatch.model.Datapoint = {
    val mockDP = mock(classOf[software.amazon.awssdk.services.cloudwatch.model.Datapoint])
    when(mockDP.unitAsString()).thenReturn("Count")
    mockDP
  }

  def mockResponse(statusCode: StatusCode, content: String = null, t: Throwable = null): Unit = {
    val httpEntity: ResponseEntity = if (content != null) {
      HttpEntity(ContentTypes.`application/json`, content)
    } else HttpEntity.Empty
    if (t == null)
      when(httpClient.singleRequest(any[HttpRequest])).thenReturn(
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
        .thenAnswer(new Answer[Future[HttpResponse]] {
          override def answer(
            invocation: org.mockito.invocation.InvocationOnMock
          ): Future[HttpResponse] = {
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
          }
        })
    }
  }

  class UTException(msg: String) extends RuntimeException(msg)
}
