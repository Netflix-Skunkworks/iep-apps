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

import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.makeFirehoseMetric
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKitBase
import com.netflix.atlas.cloudwatch.CloudWatchPoller.runKey
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import junit.framework.TestCase.assertFalse
import munit.FunSuite
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.ArgumentMatchersSugar.anyLong
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.never
import org.mockito.MockitoSugar.times
import org.mockito.MockitoSugar.verify
import org.mockito.MockitoSugar.when
import org.mockito.captor.ArgCaptor
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsResponse
import software.amazon.awssdk.services.cloudwatch.model.Metric
import software.amazon.awssdk.services.cloudwatch.paginators.ListMetricsIterable

import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.SeqHasAsJava

class CloudWatchPollerSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem("Test")

  val timestamp = Instant.ofEpochMilli(BaseCloudWatchMetricsProcessorSuite.ts)
  val account = "123456789012"
  val region = Region.US_EAST_1
  val offset = Duration.ofHours(8).getSeconds.toInt

  var registry: Registry = null
  var publishRouter: PublishRouter = null
  var processor: CloudWatchMetricsProcessor = null
  var leaderStatus: LeaderStatus = null
  var accountSupplier: AwsAccountSupplier = null
  var clientFactory: AwsClientFactory = null
  var client: CloudWatchClient = null
  var routerCaptor = ArgCaptor[FirehoseMetric]
  var debugger: CloudWatchDebugger = null
  val config = ConfigFactory.load()
  val rules: CloudWatchRules = new CloudWatchRules(config)

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    publishRouter = mock[PublishRouter]
    processor = mock[CloudWatchMetricsProcessor]
    leaderStatus = mock[LeaderStatus]
    accountSupplier = mock[AwsAccountSupplier]
    clientFactory = mock[AwsClientFactory]
    client = mock[CloudWatchClient]
    routerCaptor = ArgCaptor[FirehoseMetric]
    debugger = new CloudWatchDebugger(config, registry)

    when(leaderStatus.hasLeadership).thenReturn(true)
    when(processor.lastSuccessfulPoll(anyString)).thenReturn(0L)
    when(
      clientFactory.getInstance(
        anyString,
        any[Class[CloudWatchClient]],
        anyString,
        any[Optional[Region]]
      )
    ).thenReturn(client)
    when(accountSupplier.accounts).thenReturn(
      Map(account -> Map(Region.US_EAST_1 -> Set("AWS/UT1", "AWS/UTRedis", "AWS/UTQueryFilter")))
    )
  }

  test("init") {
    val poller = getPoller()
    val categories = poller.offsetMap(offset)
    assertEquals(categories.count(_.namespace == "AWS/UT1"), 1)
  }

  test("init multiple offsets") {
    val cfg = ConfigFactory.parseString("""
        |atlas {
        |  cloudwatch {
        |    account.polling.requestLimit = 100
        |    account.polling.fastPolling = []
        |    categories = ["cfg1", "cfg2"]
        |    poller.frequency = "5m"
        |    poller.hrmFrequency = "5s"
        |
        |    cfg1 = {
        |      namespace = "AWS/CFG1"
        |      period = 5m
        |      poll-offset = 5m
        |
        |      dimensions = ["foo"]
        |      metrics = [
        |        {
        |          name = "M1"
        |          alias = "aws.m1"
        |          conversion = "max"
        |        }
        |      ]
        |    }
        |
        |    cfg2 = {
        |      namespace = "AWS/CFG2"
        |      period = 1d
        |      poll-offset = 1h
        |
        |      dimensions = ["bar"]
        |      metrics = [
        |        {
        |          name = "M2"
        |          alias = "aws.m2"
        |          conversion = "max"
        |        }
        |      ]
        |    }
        |  }
        |}
        |""".stripMargin)
    val poller = getPoller(cfg)
    var categories = poller.offsetMap(300)
    assertEquals(categories.count(_.namespace == "AWS/CFG1"), 1)
    categories = poller.offsetMap(3600)
    assertEquals(categories.count(_.namespace == "AWS/CFG2"), 1)
  }

  test("poll not leader") {
    when(leaderStatus.hasLeadership).thenReturn(false)
    val poller = getPoller()
    poller.poll(offset, List(getCategory(poller)))
    assertCounters()
    verify(accountSupplier, never).accounts
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll already ran") {
    when(processor.lastSuccessfulPoll(anyString))
      .thenReturn(System.currentTimeMillis() + 86_400_000L)
    val poller = getPoller()
    poller.poll(offset, List(getCategory(poller)))
    assertCounters()
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll already running") {
    val poller = getPoller()
    poller.flagMap.put(runKey(28800, "123456789012", Region.US_EAST_1), new AtomicBoolean(true))
    poller.poll(offset, List(getCategory(poller)))
    assertCounters()
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll success") {
    val poller = getPoller()
    mockSuccess()
    val flag = new AtomicBoolean()
    val full = Promise[List[CloudWatchPoller#Poller]]()
    val accountsDone = Promise[Done]()
    poller.poll(offset, List(getCategory(poller)), Some(full), Some(accountsDone))

    Await.result(accountsDone.future, 60.seconds)
    val pollers = Await.result(full.future, 60.seconds)
    assertEquals(pollers.size, 1)
    assertCounters(expected = 4, polled = 4)
    assertFalse(flag.get)
    verify(processor, times(1)).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll on list failure") {
    val poller = getPoller()
    val flag = new AtomicBoolean()
    val full = Promise[List[CloudWatchPoller#Poller]]()
    val accountsDone = Promise[Done]()
    poller.poll(offset, List(getCategory(poller)), Some(full), Some(accountsDone))

    Await.result(accountsDone.future, 60.seconds)
    intercept[RuntimeException] {
      Await.result(full.future, 60.seconds)
    }
    assertCounters()
    assertFalse(flag.get)
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll on client exception") {
    when(
      clientFactory.getInstance(
        anyString,
        any[Class[CloudWatchClient]],
        anyString,
        any[Optional[Region]]
      )
    ).thenThrow(new RuntimeException("test"))
    val poller = getPoller()
    val flag = new AtomicBoolean()
    val full = Promise[List[CloudWatchPoller#Poller]]()
    val accountsDone = Promise[Done]()
    poller.poll(offset, List(getCategory(poller)), Some(full), Some(accountsDone))

    intercept[RuntimeException] {
      Await.result(accountsDone.future, 60.seconds)
    }
    intercept[RuntimeException] {
      Await.result(full.future, 60.seconds)
    }
    assertCounters(errors = Map("setup" -> 1))
    assertFalse(flag.get)
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll accounts exception") {
    when(accountSupplier.accounts).thenThrow(new RuntimeException("test"))
    val poller = getPoller()
    val flag = new AtomicBoolean()
    val full = Promise[List[CloudWatchPoller#Poller]]()
    val accountsDone = Promise[Done]()
    poller.poll(offset, List(getCategory(poller)), Some(full), Some(accountsDone))

    intercept[RuntimeException] {
      Await.result(accountsDone.future, 60.seconds)
    }
    intercept[RuntimeException] {
      Await.result(full.future, 60.seconds)
    }
    assertCounters(errors = Map("setup" -> 1))
    assertFalse(flag.get)
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("poll empty accounts") {
    when(accountSupplier.accounts).thenReturn(Map.empty)
    val poller = getPoller()
    val flag = new AtomicBoolean()
    val full = Promise[List[CloudWatchPoller#Poller]]()
    val accountsDone = Promise[Done]()
    poller.poll(offset, List(getCategory(poller)), Some(full), Some(accountsDone))

    Await.result(accountsDone.future, 60.seconds)
    Await.result(full.future, 60.seconds)
    assertCounters()
    assertFalse(flag.get)
    verify(processor, never).updateLastSuccessfulPoll(anyString, anyLong)
  }

  test("Poller#execute all success") {
    val poller = getPoller()
    mockSuccess()
    val child = getChild(poller)
    val f = child.execute
    Await.result(f, 1.seconds)
    assertCounters()
  }

  test("Poller#execute one failure") {
    val poller = getPoller()
    val child = getChild(poller)
    val f = child.execute
    intercept[RuntimeException] {
      Await.result(f, 1.seconds)
    }
    assertCounters()
  }

  test("Poller#ListMetrics success") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()
    val metrics = getListResponse(req)
    mockMetricStats()
    child.ListMetrics(req, mdef, promise).process(metrics)

    Await.result(promise.future, 1.seconds)
    assertCounters(droppedTags = 1, droppedFilter = 1)
  }

  test("Poller#ListMetrics empty") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()
    child.ListMetrics(req, mdef, promise).process(List.empty)

    assertCounters(empty = 1)
    Await.result(promise.future, 1.seconds)
  }

  test("Poller#ListMetrics one failure") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()
    val metrics = getListResponse(req)
    child.ListMetrics(req, mdef, promise).process(metrics)

    intercept[RuntimeException] {
      Await.result(promise.future, 1.seconds)
    }
    assertCounters(droppedTags = 1, droppedFilter = 1)
  }

  test("Poller#ListMetrics client throws") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()
    when(client.listMetricsPaginator(req)).thenThrow(new RuntimeException("test"))
    child.ListMetrics(req, mdef, promise).run()

    assertCounters(errors = Map("list" -> 1))
    intercept[RuntimeException] {
      Await.result(promise.future, 1.seconds)
    }
  }

  test("Poller#FetchMetricStats success") {
    val poller = getPoller()
    val child = getChild(poller)
    val category = getCategory(poller)
    val (mdef, _) = getListReq(poller)
    val promise = Promise[Done]()
    mockMetricStats()
    child.FetchMetricStats(mdef, metric, promise).run()

    verify(processor, times(2)).updateCache(
      routerCaptor,
      org.mockito.ArgumentMatchersSugar.eqTo(category),
      org.mockito.ArgumentMatchersSugar.eqTo(timestamp.toEpochMilli)
    )
    var firehose = makeFirehoseMetric(
      "AWS/UT1",
      "DailyMetricA",
      List(
        Dimension.builder().name("MyTag").value("a").build(),
        Dimension.builder().name("nf.account").value(account).build(),
        Dimension.builder().name("nf.region").value(region.toString).build()
      ),
      Array(42, 1, 5, 5),
      null,
      timestamp.minusSeconds(86400 * 2).toEpochMilli,
      ""
    )
    assertEquals(routerCaptor.values.head, firehose)

    firehose = makeFirehoseMetric(
      "AWS/UT1",
      "DailyMetricA",
      List(
        Dimension.builder().name("MyTag").value("a").build(),
        Dimension.builder().name("nf.account").value(account).build(),
        Dimension.builder().name("nf.region").value(region.toString).build()
      ),
      Array(24, 0, 3, 10),
      null,
      timestamp.minusSeconds(86400).toEpochMilli,
      ""
    )
    assertEquals(routerCaptor.values(1), firehose)
    Await.result(promise.future, 1.seconds)
    assertCounters()
  }

  test("Poller#FetchMetricStats success empty") {
    val poller = getPoller()
    val child = getChild(poller)
    val category = getCategory(poller)
    val (mdef, _) = getListReq(poller)
    val promise = Promise[Done]()
    mockMetricStats(empty = true)
    child.FetchMetricStats(mdef, metric, promise).run()

    verify(processor, never).updateCache(
      routerCaptor,
      org.mockito.ArgumentMatchersSugar.eqTo(category),
      org.mockito.ArgumentMatchersSugar.eqTo(timestamp.toEpochMilli)
    )
    Await.result(promise.future, 1.seconds)
    assertCounters()
  }

  test("Poller#FetchMetricStats client throws") {
    val poller = getPoller()
    val child = getChild(poller)
    val category = getCategory(poller)
    val (mdef, _) = getListReq(poller)
    val promise = Promise[Done]()
    mockMetricStats(exception = true)
    child.FetchMetricStats(mdef, metric, promise).run()

    verify(processor, never).updateCache(
      routerCaptor,
      org.mockito.ArgumentMatchersSugar.eqTo(category),
      org.mockito.ArgumentMatchersSugar.eqTo(timestamp.toEpochMilli)
    )
    intercept[RuntimeException] {
      Await.result(promise.future, 1.seconds)
    }
    assertCounters(errors = Map("metric" -> 1))
  }

  def getPoller(cfg: Config = config): CloudWatchPoller = {
    new CloudWatchPoller(
      cfg,
      registry,
      leaderStatus,
      accountSupplier,
      rules,
      clientFactory,
      processor,
      debugger
    )
  }

  def getChild(poller: CloudWatchPoller): CloudWatchPoller#Poller = {
    val category = poller
      .offsetMap(Duration.ofHours(8).getSeconds.toInt)
      .filter(_.namespace == "AWS/UT1")
      .head
    poller.Poller(
      timestamp,
      category,
      client,
      account,
      region,
      60
    )
  }

  def getCategory(poller: CloudWatchPoller): MetricCategory =
    poller
      .offsetMap(Duration.ofHours(8).getSeconds.toInt)
      .filter(_.namespace == "AWS/UT1")
      .head

  def getListReq(
    poller: CloudWatchPoller,
    index: Int = 0
  ): (MetricDefinition, ListMetricsRequest) = {
    val category = poller
      .offsetMap(Duration.ofHours(8).getSeconds.toInt)
      .filter(_.namespace == "AWS/UT1")
      .head
    category.toListRequests(index)
  }

  def getListResponse(req: ListMetricsRequest) = {
    List(
      Metric
        .builder()
        .namespace("AWS/UT1")
        .metricName(req.metricName())
        .dimensions(Dimension.builder().name("MyTag").value("a").build())
        .build(),
      Metric
        .builder()
        .namespace("AWS/UT1")
        .metricName(req.metricName())
        .dimensions(Dimension.builder().name("MyTag").value("b").build())
        .build(),
      Metric
        .builder()
        .namespace("AWS/UT1")
        .metricName(req.metricName())
        .dimensions(Dimension.builder().name("MyTag").value("c").build())
        .build(),
      Metric
        .builder()
        .namespace("AWS/UT1")
        .metricName(req.metricName())
        .dimensions(
          Dimension.builder().name("MyTag").value("a").build(),
          Dimension.builder().name("ExtraTag").value("filtered").build()
        )
        .build()
    )
  }

  def metric: Metric = {
    Metric
      .builder()
      .namespace("AWS/UT1")
      .metricName("DailyMetricA")
      .dimensions(Dimension.builder().name("MyTag").value("a").build())
      .build()
  }

  def mockMetricStats(empty: Boolean = false, exception: Boolean = false): Unit = {
    val data =
      if (empty) List.empty
      else
        List(
          Datapoint
            .builder()
            .sum(42)
            .minimum(1)
            .maximum(5)
            .sampleCount(5)
            .timestamp(timestamp.minusSeconds(86400 * 2))
            .build(),
          Datapoint
            .builder()
            .sum(24)
            .minimum(0)
            .maximum(3)
            .sampleCount(10)
            .timestamp(timestamp.minusSeconds(86400))
            .build()
        )
    import scala.jdk.CollectionConverters.*
    val resp = GetMetricStatisticsResponse
      .builder()
      .label("UT1")
      .datapoints(data.asJava)
      .build()
    if (exception) {
      when(client.getMetricStatistics(any[GetMetricStatisticsRequest]))
        .thenThrow(new RuntimeException("test"))
    } else {
      when(client.getMetricStatistics(any[GetMetricStatisticsRequest])).thenReturn(resp)
    }
  }

  def mockSuccess(): Unit = {
    when(client.listMetricsPaginator(any[ListMetricsRequest]))
      .thenAnswer((req: ListMetricsRequest) => {
        val metrics = List(
          Metric
            .builder()
            .namespace("AWS/UT1")
            .metricName(req.metricName())
            .dimensions(Dimension.builder().name("MyTag").value("a").build())
            .build(),
          Metric
            .builder()
            .namespace("AWS/UT1")
            .metricName(req.metricName())
            .dimensions(Dimension.builder().name("MyTag").value("b").build())
            .build()
        )
        val lmr = ListMetricsResponse
          .builder()
          .metrics(metrics.toArray*)
          .build()
        when(client.listMetrics(any[ListMetricsRequest])).thenReturn(lmr)
        new ListMetricsIterable(client, req)
      })

    val dps = List(
      Datapoint
        .builder()
        .sum(42)
        .minimum(1)
        .maximum(5)
        .sampleCount(5)
        .timestamp(timestamp.minusSeconds(86400 * 2))
        .build(),
      Datapoint
        .builder()
        .sum(24)
        .minimum(0)
        .maximum(3)
        .sampleCount(10)
        .timestamp(timestamp.minusSeconds(86400))
        .build()
    )
    val resp = GetMetricStatisticsResponse
      .builder()
      .label("UT1")
      .datapoints(dps.asJava)
      .build()
    when(client.getMetricStatistics(any[GetMetricStatisticsRequest])).thenReturn(resp)
  }

  def assertCounters(
    errors: Map[String, Long] = Map.empty,
    droppedTags: Long = 0,
    droppedFilter: Long = 0,
    empty: Long = 0,
    expected: Long = 0,
    polled: Long = 0
  ): Unit = {
    List("setup", "list", "metric").foreach { call =>
      assertEquals(
        registry
          .counter("atlas.cloudwatch.poller.failure", "call", call, "exception", "RuntimeException")
          .count(),
        errors.getOrElse(call, 0L)
      )
    }
    assertEquals(
      registry.counter("atlas.cloudwatch.poller.dps.dropped", "reason", "tags").count(),
      droppedTags
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.poller.dps.dropped", "reason", "filter").count(),
      droppedFilter
    )
    assertEquals(registry.counter("atlas.cloudwatch.poller.dps.expected").count(), expected)
    assertEquals(registry.counter("atlas.cloudwatch.poller.dps.polled").count(), polled)
    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.poller.emptyList",
          "account",
          account,
          "aws.namespace",
          "AWS/UT1",
          "region",
          region.toString
        )
        .count(),
      empty
    )
  }

}
