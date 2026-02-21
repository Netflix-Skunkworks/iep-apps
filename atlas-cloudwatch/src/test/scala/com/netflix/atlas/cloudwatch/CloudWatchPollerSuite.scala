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

import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.makeFirehoseMetric
import com.netflix.atlas.cloudwatch.CloudWatchPoller.runKey
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import junit.framework.TestCase
import junit.framework.TestCase.assertFalse
import munit.FunSuite
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKitBase
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.*
import org.mockito.stubbing.Answer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.*
import software.amazon.awssdk.services.cloudwatch.paginators.ListMetricsIterable

import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.SeqHasAsJava

class CloudWatchPollerSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem("Test")

  private val timestamp = Instant.ofEpochMilli(BaseCloudWatchMetricsProcessorSuite.ts)
  private val account = "123456789012"
  private val region = Region.US_EAST_1
  private val offset = Duration.ofHours(8).getSeconds.toInt

  private var registry: Registry = _
  private var processor: CloudWatchMetricsProcessor = _
  private var leaderStatus: LeaderStatus = _
  private var accountSupplier: AwsAccountSupplier = _
  private var clientFactory: AwsClientFactory = _
  private var client: CloudWatchClient = _
  private var routerCaptor = ArgumentCaptor.forClass(classOf[FirehoseMetric])
  private var debugger: CloudWatchDebugger = _
  private val config = ConfigFactory.load()
  private val rules: CloudWatchRules = new CloudWatchRules(config)

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    processor = mock(classOf[CloudWatchMetricsProcessor])
    leaderStatus = mock(classOf[LeaderStatus])
    accountSupplier = mock(classOf[AwsAccountSupplier])
    clientFactory = mock(classOf[AwsClientFactory])
    client = mock(classOf[CloudWatchClient])
    routerCaptor = ArgumentCaptor.forClass(classOf[FirehoseMetric])
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
    // Mock updateCache to return a successful Future
    when(processor.updateCache(any[FirehoseMetric], any[MetricCategory], anyLong))
      .thenReturn(Future.successful(()))
  }

  /**
   * Helper to exercise Poller#ListMetrics with an explicit metrics list.
   */
  private def runListMetricsWithMetrics(
    child: CloudWatchPoller#Poller,
    mdef: MetricDefinition,
    req: ListMetricsRequest,
    metrics: List[Metric],
    promise: Promise[Done]
  ): Unit = {
    when(client.listMetricsPaginator(req))
      .thenAnswer(new Answer[ListMetricsIterable] {
        override def answer(
          invocation: org.mockito.invocation.InvocationOnMock
        ): ListMetricsIterable = {
          val r = invocation.getArgument(0, classOf[ListMetricsRequest])

          val lmr = ListMetricsResponse
            .builder()
            .metrics(metrics.toArray*)
            .build()

          when(client.listMetrics(any[ListMetricsRequest])).thenReturn(lmr)

          new ListMetricsIterable(client, r)
        }
      })

    // Drive the ListMetrics runnable (which now calls processConcreteMetrics internally)
    child.ListMetrics(req, mdef, promise).run()
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
        |    account.polling.fastBatchPolling = []
        |    categories = ["cfg1", "cfg2"]
        |    poller.frequency = "5m"
        |    poller.hrmFrequency = "5s"
        |    poller.hrmListFrequency = "5s"
        |    poller.useHrmMetricsCache = false
        |    poller.hrmLookback = 6
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
        |          unit = "Megabits/Second"
        |        }
        |      ]
        |    }
        |  }
        |}
        |""".stripMargin)
    val poller = getPoller(cfg)
    var categories = poller.offsetMap(300)
    assertEquals(categories.count(_.namespace == "AWS/CFG1"), 1)
    categories.foreach { category =>
      category.metrics.foreach { metric =>
        val unit = metric.unit
        TestCase.assertEquals(unit, StandardUnit.NONE.toString)
      }
    }

    categories = poller.offsetMap(3600)
    assertEquals(categories.count(_.namespace == "AWS/CFG2"), 1)

    categories.foreach { category =>
      category.metrics.foreach { metric =>
        val unit = metric.unit
        TestCase.assertEquals(unit, StandardUnit.MEGABITS_SECOND.toString)
      }
    }
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

    // Validate the unit for each metric in the configuration
    val categories = poller.offsetMap(offset)
    categories.foreach { category =>
      category.metrics.foreach { metric =>
        val unit = metric.unit
        if (unit != null && StandardUnit.values().contains(StandardUnit.fromValue(unit))) {
          // Valid unit
          assert(true)
        } else {
          // Invalid or missing unit
          assert(false, s"Invalid or missing unit for metric ${metric.name}")
        }
      }
    }

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
    Await.result(f, 60.seconds)
    assertCounters()
  }

  test("Poller#execute one failure") {
    val poller = getPoller()
    val child = getChild(poller)
    val f = child.execute
    intercept[RuntimeException] {
      Await.result(f, 60.seconds)
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
    runListMetricsWithMetrics(child, mdef, req, metrics, promise)

    Await.result(promise.future, 60.seconds)
    assertCounters(droppedTags = 1, droppedFilter = 1)
  }

  test("Poller#ListMetrics empty") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()
    runListMetricsWithMetrics(child, mdef, req, List.empty, promise)
    assertCounters(empty = 1)
    Await.result(promise.future, 60.seconds)
  }

  test("Poller#ListMetrics one failure") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()
    val metrics = getListResponse(req)
    runListMetricsWithMetrics(child, mdef, req, metrics, promise)

    intercept[RuntimeException] {
      Await.result(promise.future, 60.seconds)
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
      Await.result(promise.future, 60.seconds)
    }
  }

  test("ListMetrics populates cache and UseCachedMetrics reuses it") {
    val poller = getPoller()
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)

    val metrics = getListResponse(req)

    // --- First run: ListMetrics, should populate cache ---

    val p1 = Promise[Done]()

    when(client.listMetricsPaginator(req))
      .thenAnswer(new Answer[ListMetricsIterable] {
        override def answer(
          invocation: org.mockito.invocation.InvocationOnMock
        ): ListMetricsIterable = {
          val r = invocation.getArgument(0, classOf[ListMetricsRequest])
          val lmr = ListMetricsResponse
            .builder()
            .metrics(metrics.toArray*)
            .build()
          when(client.listMetrics(any[ListMetricsRequest])).thenReturn(lmr)
          new ListMetricsIterable(client, r)
        }
      })

    mockMetricStats()

    child.ListMetrics(req, mdef, p1).run()
    Await.result(p1.future, 60.seconds)

    reset(client)
    when(client.getMetricStatistics(any[GetMetricStatisticsRequest]))
      .thenReturn(
        GetMetricStatisticsResponse
          .builder()
          .label("UT1")
          .datapoints(java.util.Collections.emptyList[Datapoint]())
          .build()
      )

    // --- Second run: UseCachedMetrics, should not call listMetricsPaginator ---

    val p2 = Promise[Done]()

    child.UseCachedMetrics(mdef, metrics, p2).run()
    Await.result(p2.future, 60.seconds)

    // Verify that no new listMetricsPaginator calls happened in cached path
    verify(client, never()).listMetricsPaginator(any[ListMetricsRequest])
  }

  test("HRM lookback dedupes datapoints via highResTimeCache") {
    // HRM config: period 1s, hrmLookback 6, hrmFrequency 3s (values not critical for this unit test)
    val cfg = ConfigFactory
      .parseString(
        """
          |atlas {
          |  cloudwatch {
          |    account.polling.requestLimit = 100
          |    account.polling.fastPolling = []
          |    account.polling.fastBatchPolling = []
          |    categories = ["ut-hrm-dedupe"]
          |    poller.frequency = "5m"
          |    poller.hrmFrequency = "3s"
          |    poller.hrmLookback = 6
          |
          |    ut-hrm-dedupe = {
          |      namespace = "AWS/UT1"
          |      period = 1s
          |      poll-offset = 8h
          |
          |      dimensions = ["MyTag"]
          |      metrics = [
          |        {
          |          name = "HRMMetric"
          |          alias = "aws.ut1.hrm.dedupe"
          |          conversion = "sum,rate"
          |        }
          |      ]
          |    }
          |  }
          |}
          |""".stripMargin
      )
      .withFallback(config)

    // Re-mock processor so we can capture sendToRegistry calls for this test
    processor = mock(classOf[CloudWatchMetricsProcessor])
    val registry2 = new DefaultRegistry()
    val poller2 = new CloudWatchPoller(
      cfg,
      registry2,
      leaderStatus,
      accountSupplier,
      rules,
      clientFactory,
      processor,
      debugger
    )

    // Use the HRM category from this poller
    val category = poller2
      .offsetMap(Duration.ofHours(8).getSeconds.toInt)
      .find(_.namespace == "AWS/UT1")
      .getOrElse(sys.error("HRM category not found"))

    val child = poller2.Poller(timestamp, category, client, account, region, 60)
    val (mdef, _) = getListReq(poller2)

    // Build a metric and 3 HRM datapoints (t, t-1, t-2 seconds)
    val m = Metric
      .builder()
      .namespace("AWS/UT1")
      .metricName("HRMMetric")
      .dimensions(Dimension.builder().name("MyTag").value("a").build())
      .build()

    val ts0 = timestamp.minusSeconds(2)
    val ts1 = timestamp.minusSeconds(1)
    val ts2 = timestamp

    def dp(ts: Instant, v: Double): Datapoint =
      Datapoint
        .builder()
        .sum(v)
        .minimum(v)
        .maximum(v)
        .sampleCount(1.0)
        .timestamp(ts)
        .build()

    val firstBatch = List(
      dp(ts2, 10.0),
      dp(ts1, 20.0),
      dp(ts0, 30.0)
    )

    // Second batch includes the same three timestamps plus one new one (ts3)
    val ts3 = timestamp.plusSeconds(1)
    val secondBatch = firstBatch :+ dp(ts3, 40.0)

    val metaCaptor = ArgumentCaptor.forClass(classOf[MetricMetadata])
    val fmCaptor = ArgumentCaptor.forClass(classOf[FirehoseMetric])

    // First call: process firstBatch → all 3 datapoints should be sent
    child.processMetricDatapoints(mdef, m, firstBatch, ts2.toEpochMilli)
    verify(processor, times(3)).sendToRegistry(
      metaCaptor.capture(),
      fmCaptor.capture(),
      anyLong()
    )

    // Reset captors & mocks for second invocation
    reset(processor)

    val metaCaptor2 = ArgumentCaptor.forClass(classOf[MetricMetadata])
    val fmCaptor2 = ArgumentCaptor.forClass(classOf[FirehoseMetric])

    // Second call: process secondBatch (3 old + 1 new)
    // Only the new timestamp (ts3) should be forwarded due to highResTimeCache dedupe
    child.processMetricDatapoints(mdef, m, secondBatch, ts3.toEpochMilli)

    verify(processor, times(1)).sendToRegistry(
      metaCaptor2.capture(),
      fmCaptor2.capture(),
      anyLong()
    )

    val sent = fmCaptor2.getValue
    assertEquals(sent.datapoint.timestamp(), ts3)
  }

  test("HRM datapoints built correctly with GetMetricData batch (period < 60)") {
    // Config with this account in fastBatchPollingAccounts and HRM period
    val cfg = ConfigFactory
      .parseString(
        """
          |atlas {
          |  cloudwatch {
          |    account.polling.requestLimit = 100
          |    account.polling.fastPolling = []
          |    account.polling.fastBatchPolling = ["123456789012"]
          |    categories = ["ut-hrm"]
          |    poller.frequency = "5m"
          |    poller.hrmFrequency = "5s"
          |    poller.hrmLookback = 2
          |
          |    ut-hrm = {
          |      namespace = "AWS/UT1"
          |      period = 5s
          |      poll-offset = 8h
          |
          |      dimensions = ["MyTag"]
          |      metrics = [
          |        {
          |          name = "DailyMetricA"
          |          alias = "aws.ut1.hrm"
          |          conversion = "max"
          |        }
          |      ]
          |    }
          |  }
          |}
          |""".stripMargin
      )
      .withFallback(config)

    val poller = getPoller(cfg)
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()

    // listMetrics returns one metric with MyTag=a
    when(client.listMetricsPaginator(any[ListMetricsRequest]))
      .thenAnswer(new Answer[ListMetricsIterable] {
        override def answer(
          invocation: org.mockito.invocation.InvocationOnMock
        ): ListMetricsIterable = {
          val r = invocation.getArgument(0, classOf[ListMetricsRequest])
          val metrics = List(
            Metric
              .builder()
              .namespace("AWS/UT1")
              .metricName(r.metricName())
              .dimensions(Dimension.builder().name("MyTag").value("a").build())
              .build()
          )
          val lmr = ListMetricsResponse
            .builder()
            .metrics(metrics.toArray*)
            .build()
          when(client.listMetrics(any[ListMetricsRequest])).thenReturn(lmr)
          new ListMetricsIterable(client, r)
        }
      })

    // Build GetMetricDataResponse series equivalent to mockMetricStats HRM datapoints
    val tsList = java.util.Arrays.asList(
      timestamp.minusSeconds(10),
      timestamp
    )

    def res(id: String, v1: Double, v2: Double) =
      MetricDataResult
        .builder()
        .id(id)
        .timestamps(tsList)
        .values(java.util.Arrays.asList(v1: java.lang.Double, v2: java.lang.Double))
        .build()

    val results = java.util.Arrays.asList(
      res("m0_max", 20.0, 25.0),
      res("m0_min", 10.0, 15.0),
      res("m0_sum", 100.0, 200.0),
      res("m0_cnt", 5.0, 7.0)
    )

    val gmdResp = GetMetricDataResponse
      .builder()
      .metricDataResults(results)
      .build()

    when(client.getMetricData(any[GetMetricDataRequest])).thenReturn(gmdResp)

    // Exercise ListMetrics path, which for fastBatch account will route to GetMetricDataBatch
    child.ListMetrics(req, mdef, promise).run()
    Await.result(promise.future, 60.seconds)

    val metaCaptor = ArgumentCaptor.forClass(classOf[MetricMetadata])
    val fmCaptor = ArgumentCaptor.forClass(classOf[FirehoseMetric])

    // Expect two datapoints, like non-batch HRM test
    verify(processor, times(2)).sendToRegistry(
      metaCaptor.capture(),
      fmCaptor.capture(),
      anyLong()
    )

    val firehoses = fmCaptor.getAllValues

    val f0 = firehoses.get(0)
    assertEquals(f0.namespace, "AWS/UT1")
    assertEquals(f0.metricName, "DailyMetricA")
    assertEquals(f0.datapoint.sum(), java.lang.Double.valueOf(100.0))
    assertEquals(f0.datapoint.minimum(), java.lang.Double.valueOf(10.0))
    assertEquals(f0.datapoint.maximum(), java.lang.Double.valueOf(20.0))
    assertEquals(f0.datapoint.sampleCount(), java.lang.Double.valueOf(5.0))

    val dims0 = f0.dimensions
    assert(dims0.contains(Dimension.builder().name("MyTag").value("a").build()))
    assert(dims0.contains(Dimension.builder().name("nf.account").value(account).build()))
    assert(dims0.contains(Dimension.builder().name("nf.region").value(region.toString).build()))

    val f1 = firehoses.get(1)
    assertEquals(f1.datapoint.sum(), java.lang.Double.valueOf(200.0))
    assertEquals(f1.datapoint.minimum(), java.lang.Double.valueOf(15.0))
    assertEquals(f1.datapoint.maximum(), java.lang.Double.valueOf(25.0))
    assertEquals(f1.datapoint.sampleCount(), java.lang.Double.valueOf(7.0))
  }

  test("HRM datapoints built correctly with GetMetricStatistics (period < 60)") {
    // Configure a high-res category (period 5s)
    val cfg = ConfigFactory
      .parseString(
        """
          |atlas {
          |  cloudwatch {
          |    account.polling.requestLimit = 100
          |    account.polling.fastPolling = []
          |    account.polling.fastBatchPolling = []
          |    categories = ["ut-hrm"]
          |    poller.frequency = "5m"
          |    poller.hrmFrequency = "5s"
          |    poller.hrmLookback = 2
          |
          |    ut-hrm = {
          |      namespace = "AWS/UT1"
          |      period = 5s
          |      poll-offset = 8h
          |
          |      dimensions = ["MyTag"]
          |      metrics = [
          |        {
          |          name = "DailyMetricA"
          |          alias = "aws.ut1.hrm"
          |          conversion = "max"
          |        }
          |      ]
          |    }
          |  }
          |}
          |""".stripMargin
      )
      .withFallback(config)

    val poller = getPoller(cfg)
    val child = getChild(poller)
    val (mdef, _) = getListReq(poller)
    val promise = Promise[Done]()

    // HRM-style datapoints from GetMetricStatistics (one old, one recent)
    val dps = List(
      Datapoint
        .builder()
        .sum(100.0)
        .minimum(10.0)
        .maximum(20.0)
        .sampleCount(5.0)
        .timestamp(timestamp.minusSeconds(10)) // older
        .build(),
      Datapoint
        .builder()
        .sum(200.0)
        .minimum(15.0)
        .maximum(25.0)
        .sampleCount(7.0)
        .timestamp(timestamp) // latest
        .build()
    )

    val resp = GetMetricStatisticsResponse
      .builder()
      .label("UT1")
      .datapoints(dps.asJava)
      .build()

    when(client.getMetricStatistics(any[GetMetricStatisticsRequest])).thenReturn(resp)

    child.FetchMetricStats(mdef, metric, promise).run()
    Await.result(promise.future, 60.seconds)

    // Verify sendToRegistry was called twice with the correct datapoints
    val metaCaptor = ArgumentCaptor.forClass(classOf[MetricMetadata])
    val fmCaptor = ArgumentCaptor.forClass(classOf[FirehoseMetric])

    verify(processor, times(2)).sendToRegistry(
      metaCaptor.capture(),
      fmCaptor.capture(),
      anyLong()
    )

    val firehoses = fmCaptor.getAllValues

    // Check first datapoint
    val f0 = firehoses.get(0)
    assertEquals(f0.namespace, "AWS/UT1")
    assertEquals(f0.metricName, "DailyMetricA") // from metric.metricName()
    assertEquals(f0.datapoint.sum(), java.lang.Double.valueOf(100.0))
    assertEquals(f0.datapoint.minimum(), java.lang.Double.valueOf(10.0))
    assertEquals(f0.datapoint.maximum(), java.lang.Double.valueOf(20.0))
    assertEquals(f0.datapoint.sampleCount(), java.lang.Double.valueOf(5.0))

    val dims0 = f0.dimensions
    assert(dims0.contains(Dimension.builder().name("MyTag").value("a").build()))
    assert(dims0.contains(Dimension.builder().name("nf.account").value(account).build()))
    assert(dims0.contains(Dimension.builder().name("nf.region").value(region.toString).build()))

    // Check second datapoint
    val f1 = firehoses.get(1)
    assertEquals(f1.datapoint.sum(), java.lang.Double.valueOf(200.0))
    assertEquals(f1.datapoint.minimum(), java.lang.Double.valueOf(15.0))
    assertEquals(f1.datapoint.maximum(), java.lang.Double.valueOf(25.0))
    assertEquals(f1.datapoint.sampleCount(), java.lang.Double.valueOf(7.0))
  }

  test("HRM lookback dedupes datapoints via highResTimeCache with bounded retention") {
    // HRM config: period 1s, hrmLookback 6, hrmFrequency 3s
    val cfg = ConfigFactory
      .parseString(
        """
          |atlas {
          |  cloudwatch {
          |    account.polling.requestLimit = 100
          |    account.polling.fastPolling = []
          |    account.polling.fastBatchPolling = []
          |    categories = ["ut-hrm-dedupe"]
          |    poller.frequency = "5m"
          |    poller.hrmFrequency = "3s"
          |    poller.hrmLookback = 6
          |    poller.hrmListFrequency = "5s"
          |    poller.useHrmMetricsCache = false
          |
          |    ut-hrm-dedupe = {
          |      namespace = "AWS/UT1"
          |      period = 1s
          |      poll-offset = 8h
          |
          |      dimensions = ["MyTag"]
          |      metrics = [
          |        {
          |          name = "HRMMetric"
          |          alias = "aws.ut1.hrm.dedupe"
          |          conversion = "sum,rate"
          |        }
          |      ]
          |    }
          |  }
          |}
          |""".stripMargin
      )
      .withFallback(config)

    // Re-mock processor so we can capture sendToRegistry calls for this test
    processor = mock(classOf[CloudWatchMetricsProcessor])
    val registry2 = new DefaultRegistry()
    val poller2 = new CloudWatchPoller(
      cfg,
      registry2,
      leaderStatus,
      accountSupplier,
      rules,
      clientFactory,
      processor,
      debugger
    )

    // HRM category from this poller
    val category = poller2
      .offsetMap(Duration.ofHours(8).getSeconds.toInt)
      .find(_.namespace == "AWS/UT1")
      .getOrElse(sys.error("HRM category not found"))

    val child = poller2.Poller(timestamp, category, client, account, region, 60)
    val (mdef, _) = getListReq(poller2)

    // Metric with a single dimension
    val m = Metric
      .builder()
      .namespace("AWS/UT1")
      .metricName("HRMMetric")
      .dimensions(Dimension.builder().name("MyTag").value("a").build())
      .build()

    // Three datapoints at t-2, t-1, t
    val ts0 = timestamp.minusSeconds(2)
    val ts1 = timestamp.minusSeconds(1)
    val ts2 = timestamp

    def dp(ts: Instant, v: Double): Datapoint =
      Datapoint
        .builder()
        .sum(v)
        .minimum(v)
        .maximum(v)
        .sampleCount(1.0)
        .timestamp(ts)
        .build()

    val firstBatch = List(
      dp(ts0, 10.0),
      dp(ts1, 20.0),
      dp(ts2, 30.0)
    )

    // Second batch includes the same three timestamps plus one new one at t+1
    val ts3 = timestamp.plusSeconds(1)
    val secondBatch = firstBatch :+ dp(ts3, 40.0)

    val metaCaptor1 = ArgumentCaptor.forClass(classOf[MetricMetadata])
    val fmCaptor1 = ArgumentCaptor.forClass(classOf[FirehoseMetric])

    // First call: all 3 datapoints are new → all 3 should be sent
    child.processMetricDatapoints(mdef, m, firstBatch, ts2.toEpochMilli)
    verify(processor, times(3)).sendToRegistry(
      metaCaptor1.capture(),
      fmCaptor1.capture(),
      anyLong()
    )

    // Reset mocks for the second invocation
    reset(processor)

    val metaCaptor2 = ArgumentCaptor.forClass(classOf[MetricMetadata])
    val fmCaptor2 = ArgumentCaptor.forClass(classOf[FirehoseMetric])

    // Second call: three old + one new timestamp
    // Only the new timestamp (ts3) should be forwarded due to per-(series,ts) dedupe
    child.processMetricDatapoints(mdef, m, secondBatch, ts3.toEpochMilli)

    verify(processor, times(1)).sendToRegistry(
      metaCaptor2.capture(),
      fmCaptor2.capture(),
      anyLong()
    )

    val sent = fmCaptor2.getValue
    assertEquals(sent.datapoint.timestamp(), ts3)
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
      routerCaptor.capture(),
      org.mockito.ArgumentMatchers.eq(category),
      org.mockito.ArgumentMatchers.eq(timestamp.toEpochMilli)
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
    assertEquals(routerCaptor.getAllValues.get(0), firehose)

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
    assertEquals(routerCaptor.getAllValues.get(1), firehose)
    Await.result(promise.future, 60.seconds)
    assertCounters()
  }

  test("non-fastBatch account uses GetMetricStatistics") {
    // Config with no fastBatch accounts
    val cfg = ConfigFactory
      .parseString(
        """
        |atlas {
        |  cloudwatch {
        |    account.polling.requestLimit = 100
        |    account.polling.fastPolling = []
        |    account.polling.fastBatchPolling = []
        |  }
        |}
        |""".stripMargin
      )
      .withFallback(config)

    val poller = getPoller(cfg)
    val child = getChild(poller)
    val (mdef, _) = getListReq(poller)
    val promise = Promise[Done]()

    mockMetricStats() // uses getMetricStatistics

    // Directly test FetchMetricStats path
    child.FetchMetricStats(mdef, metric, promise).run()
    Await.result(promise.future, 60.seconds)

    // Ensure GetMetricStatistics was used
    verify(client, atLeastOnce()).getMetricStatistics(any[GetMetricStatisticsRequest])
    // And we did not call GetMetricData for this path
    verify(client, never()).getMetricData(any[GetMetricDataRequest])

    assertCounters()
  }

  test("fastBatch account uses GetMetricData, not GetMetricStatistics") {
    // Config with this account in fastBatchPollingAccounts
    val cfg = ConfigFactory
      .parseString(
        """
        |atlas {
        |  cloudwatch {
        |    account.polling.requestLimit = 100
        |    account.polling.fastPolling = []
        |    account.polling.fastBatchPolling = ["123456789012"]
        |    categories = ["ut1"]
        |    poller.frequency = "5m"
        |    poller.hrmFrequency = "5m"
        |    poller.hrmLookback = 6
        |
        |    ut1 = {
        |      namespace = "AWS/UT1"
        |      period = 1d
        |      poll-offset = 8h
        |
        |      dimensions = ["MyTag"]
        |      metrics = [
        |        {
        |          name = "DailyMetricA"
        |          alias = "aws.ut1.daily"
        |          conversion = "max"
        |        }
        |      ]
        |    }
        |  }
        |}
        |""".stripMargin
      )
      .withFallback(config)

    val poller = getPoller(cfg)
    val child = getChild(poller)
    val (mdef, req) = getListReq(poller)
    val promise = Promise[Done]()

    // listMetrics paginator returns 2 metrics as in mockSuccess, but we mock it ourselves here:
    when(client.listMetricsPaginator(any[ListMetricsRequest]))
      .thenAnswer(new Answer[ListMetricsIterable] {
        override def answer(
          invocation: org.mockito.invocation.InvocationOnMock
        ): ListMetricsIterable = {
          val r = invocation.getArgument(0, classOf[ListMetricsRequest])
          val metrics = List(
            Metric
              .builder()
              .namespace("AWS/UT1")
              .metricName(r.metricName())
              .dimensions(Dimension.builder().name("MyTag").value("a").build())
              .build(),
            Metric
              .builder()
              .namespace("AWS/UT1")
              .metricName(r.metricName())
              .dimensions(Dimension.builder().name("MyTag").value("b").build())
              .build()
          )
          val lmr = ListMetricsResponse
            .builder()
            .metrics(metrics.toArray*)
            .build()
          when(client.listMetrics(any[ListMetricsRequest])).thenReturn(lmr)
          new ListMetricsIterable(client, r)
        }
      })

    // Mock GetMetricData response with a single datapoint for each stat, both metrics
    val nowTs = timestamp
    val tsList = java.util.Arrays.asList(nowTs, nowTs.plusSeconds(60))

    def result(id: String, values: java.util.List[java.lang.Double]) =
      software.amazon.awssdk.services.cloudwatch.model.MetricDataResult
        .builder()
        .id(id)
        .timestamps(tsList)
        .values(values)
        .build()

    // For 2 metrics (m0, m1) and 4 stats each = 8 results
    val results = java.util.Arrays.asList(
      result("m0_max", java.util.Arrays.asList(5.0, 6.0)),
      result("m0_min", java.util.Arrays.asList(1.0, 2.0)),
      result("m0_sum", java.util.Arrays.asList(10.0, 12.0)),
      result("m0_cnt", java.util.Arrays.asList(2.0, 2.0)),
      result("m1_max", java.util.Arrays.asList(7.0, 8.0)),
      result("m1_min", java.util.Arrays.asList(0.0, 1.0)),
      result("m1_sum", java.util.Arrays.asList(14.0, 16.0)),
      result("m1_cnt", java.util.Arrays.asList(2.0, 2.0))
    )

    val gmdResp = software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse
      .builder()
      .metricDataResults(results)
      .build()

    when(client.getMetricData(any[GetMetricDataRequest])).thenReturn(gmdResp)

    // Run the ListMetrics path: for fastBatch account this should use GetMetricDataBatch
    child.ListMetrics(req, mdef, promise).run()
    Await.result(promise.future, 60.seconds)

    // Verify we never called GetMetricStatistics on fastBatch path
    verify(client, never()).getMetricStatistics(any[GetMetricStatisticsRequest])

    // We should have called GetMetricData once
    verify(client, times(1)).getMetricData(any[GetMetricDataRequest])

    // Optionally, assert no error counters
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
      routerCaptor.capture(),
      org.mockito.ArgumentMatchers.eq(category),
      org.mockito.ArgumentMatchers.eq(timestamp.toEpochMilli)
    )
    Await.result(promise.future, 60.seconds)
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
      routerCaptor.capture(),
      org.mockito.ArgumentMatchers.eq(category),
      org.mockito.ArgumentMatchers.eq(timestamp.toEpochMilli)
    )
    intercept[RuntimeException] {
      Await.result(promise.future, 6022.seconds)
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

  def getListResponse(req: ListMetricsRequest): List[Metric] = {
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
      .thenAnswer((invocation: org.mockito.invocation.InvocationOnMock) => {
        val req = invocation.getArgument(0, classOf[ListMetricsRequest])
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
