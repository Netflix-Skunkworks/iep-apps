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
import akka.testkit.ImplicitSender
import akka.testkit.TestKitBase
import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newCacheEntry
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newValue
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDatapoint
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDimensions
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.assertAWSDP
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.assertCWDP
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.makeDatapoint
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.makeFirehoseMetric
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.timestamp
import com.netflix.atlas.core.model.Query
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.Assertions.assertEquals
import munit.FunSuite
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.spy
import org.mockito.MockitoSugar.times
import org.mockito.MockitoSugar.verify
import org.mockito.captor.ArgCaptor
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import java.time.Instant

class CloudWatchMetricsProcessorSuite extends FunSuite with TestKitBase with ImplicitSender {

  override implicit def system: ActorSystem = ActorSystem("Test")

  var registry: Registry = null
  var publishRouter: PublishRouter = null
  var processor: CloudWatchMetricsProcessor = null
  var routerCaptor = ArgCaptor[AtlasDatapoint]
  val config = ConfigFactory.load()
  val tagger = new NetflixTagger(config.getConfig("atlas.cloudwatch.tagger"))
  val rules: CloudWatchRules = new CloudWatchRules(config)
  val category = MetricCategory("AWS/DynamoDB", 60, 0, 2, None, List("MyTag"), List.empty, null)

  val cwDP = newCacheEntry(
    makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp),
    category
  )

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    publishRouter = mock[PublishRouter]
    processor = new LocalCloudWatchMetricsProcessor(config, registry, rules, tagger, publishRouter)
    routerCaptor = ArgCaptor[AtlasDatapoint]
  }

  test("processDatapoints Empty") {
    val hash = makeFirehoseMetric(Array(1.0, 1.0, 1.0, 1), timestamp).xxHash
    processor
      .asInstanceOf[LocalCloudWatchMetricsProcessor]
      .inject(hash, cwDP.toBuilder.clearData().build().toByteArray, timestamp, 3600)

    assertPublished(List.empty, timestamp + (60_000 * 10))
    assertCounters(0, publishEmpty = 1)
  }

  test("processDatapoints No namespace config") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/SomeNoneExistingNS",
          "MyMetric",
          List.empty,
          Array(1.0, 1.0, 1.0, 1),
          "Bytes"
        )
      ),
      timestamp
    )
    assertPublished(List.empty)
    assertCounters(1, filtered = Map("namespace" -> 1))
  }

  test("processDatapoints no metric match") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "SomeUnknownMetric",
          List(Dimension.builder().name("Key").value("Value").build()),
          Array(39.0, 1.0, 7.0, 19),
          "Count"
        )
      ),
      timestamp
    )
    assertPublished(List.empty)
    assertCounters(1, filtered = Map("metric" -> 1))
  }

  test("processDatapoints missing tag") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "SumRate",
          List(Dimension.builder().name("NotTheRightTag").value("Whoops").build()),
          Array(39.0, 1.0, 7.0, 19),
          "Count"
        )
      ),
      timestamp
    )
    assertPublished(List.empty)
    assertCounters(1, filtered = Map("tags" -> 1))
  }

  test("processDatapoints Filter by query") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UTQueryFilter",
          "MyTestMetric",
          List(Dimension.builder().name("Key").value("Whoops").build()),
          Array(39.0, 1.0, 7.0, 19),
          "Count"
        )
      ),
      timestamp
    )
    assertPublished(List.empty)
    assertCounters(1, filtered = Map("query" -> 1))
  }

  test("processDatapoints matched dist-summary") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "Dist",
          List(Dimension.builder().name("MyTag").value("Val").build()),
          Array(39.0, 1.0, 7.0, 19),
          "Count",
          timestamp - 60_000
        )
      ),
      timestamp - 60_000
    )

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.dist",
            "statistic"    -> "count",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          0.31666666666666665
        ),
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.dist",
            "statistic"    -> "totalAmount",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          0.65
        ),
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.dist",
            "statistic"    -> "max",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "gauge"
          ),
          timestamp,
          7.0
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints matched timer") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "Timer",
          List(
            Dimension.builder().name("LoadBalancerName").value("some-elb").build(),
            Dimension.builder().name("AvailabilityZone").value("a").build()
          ),
          Array(0.1226732730865479, 0.1226732730865479, 0.1226732730865479, 1),
          "Timer",
          timestamp - 60_000
        )
      ),
      timestamp - 60_000
    )

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.timer",
            "aws.elb"      -> "some-elb",
            "statistic"    -> "count",
            "nf.region"    -> "us-west-2",
            "nf.cluster"   -> "some-elb",
            "nf.stack"     -> "elb",
            "nf.app"       -> "some",
            "nf.zone"      -> "a",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          0.016666666666666666
        ),
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.timer",
            "aws.elb"      -> "some-elb",
            "statistic"    -> "totalTime",
            "nf.region"    -> "us-west-2",
            "nf.cluster"   -> "some-elb",
            "nf.stack"     -> "elb",
            "nf.app"       -> "some",
            "nf.zone"      -> "a",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          0.0020445545514424647
        ),
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.timer",
            "aws.elb"      -> "some-elb",
            "statistic"    -> "max",
            "nf.region"    -> "us-west-2",
            "nf.cluster"   -> "some-elb",
            "nf.stack"     -> "elb",
            "nf.app"       -> "some",
            "nf.zone"      -> "a",
            "atlas.dstype" -> "gauge"
          ),
          timestamp,
          0.1226732730865479
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints matched bytes") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "Bytes",
          List(
            Dimension.builder().name("LoadBalancerName").value("some-elb").build(),
            Dimension.builder().name("AvailabilityZone").value("a").build()
          ),
          Array(1.6903156e7, 8378272.0, 8524884.0, 2),
          "Bytes",
          timestamp - 60_000
        )
      ),
      timestamp - 60_000
    )

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.bytes",
            "aws.elb"      -> "some-elb",
            "nf.region"    -> "us-west-2",
            "nf.zone"      -> "a",
            "nf.cluster"   -> "some-elb",
            "nf.stack"     -> "elb",
            "nf.app"       -> "some",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          281719.26666666666
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints matched percent") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "Max",
          List(Dimension.builder().name("MyTag").value("Val").build()),
          Array(4.1320470343685605, 4.1320470343685605, 4.1320470343685605, 1),
          "Percent",
          timestamp - 60_000
        )
      ),
      timestamp - 60_000
    )

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.max",
            "aws.tag"      -> "Val",
            "nf.region"    -> "us-west-2",
            "atlas.dstype" -> "gauge"
          ),
          timestamp,
          4.1320470343685605
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints config with timeout") {
    // 15m timeout so we make sure the data sticks around that long at least and we keep posting the last
    // value.
    var dp = makeFirehoseMetric(
      "AWS/UT1",
      "TimeOut",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      Array(1, 1, 1, 1),
      "None",
      timestamp - (60_000 * 10)
    )
    processor.processDatapoints(List(dp), timestamp - 60_000)

    dp = dp.copy(datapoint = makeDatapoint(Array(2, 2, 2, 2), timestamp - (60_000 * 5)))
    processor.processDatapoints(List(dp), timestamp)

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.timeout",
            "aws.tag"      -> "Val",
            "nf.region"    -> "us-west-2",
            "atlas.dstype" -> "gauge"
          ),
          timestamp,
          2
        )
      )
    )
    assertCounters(2)
  }

  test("processDatapoints config with end period offset") {
    // 1 end period offset so we back date.
    val dp = makeFirehoseMetric(
      "AWS/UT1",
      "Offset",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      Array(1, 1, 1, 1),
      "None",
      timestamp
    )
    processor.processDatapoints(List(dp), timestamp)
    processor.processDatapoints(
      List(
        dp.copy(
          datapoint = makeDatapoint(Array(2, 2, 2, 1), timestamp + 60_000, "None")
        )
      ),
      timestamp + 60_000
    )

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.offset",
            "aws.tag"      -> "Val",
            "nf.region"    -> "us-west-2",
            "atlas.dstype" -> "gauge"
          ),
          timestamp + 60_000,
          1
        )
      ),
      timestamp + 60_000
    )
    assertCounters(2)
  }

  test("processDatapoints config with monotonic") {
    var dp = makeFirehoseMetric(
      "AWS/UT1",
      "Mono",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      Array(60, 60, 60, 1),
      "None",
      timestamp - (60_000 * 2)
    )
    processor.processDatapoints(List(dp), timestamp - (60_000 * 2))

    dp = dp.copy(datapoint = makeDatapoint(Array(120, 120, 120, 1), timestamp - 60_000))
    processor.processDatapoints(List(dp), timestamp - 60_000)

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.mono",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          1.0
        )
      )
    )
    assertCounters(2)
  }

  test("processDatapoints purged namespace") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(Map.empty)
    processor =
      new LocalCloudWatchMetricsProcessor(config, registry, ruleSpy, tagger, publishRouter)
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp)),
      timestamp
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("namespace" -> 1))
  }

  test("processDatapoints purged metric") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(Map("AWS/UT1" -> Map("SomeMetric" -> (category, List.empty))))
    processor =
      new LocalCloudWatchMetricsProcessor(config, registry, ruleSpy, tagger, publishRouter)
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp)),
      timestamp
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("metric" -> 1))
  }

  test("processDatapoints purged tags") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(
        Map(
          "AWS/UT1" -> Map(
            "SumRate" ->
              (category.copy(dimensions = List("SomeOtherTag")), List.empty)
          )
        )
      )
    processor =
      new LocalCloudWatchMetricsProcessor(config, registry, ruleSpy, tagger, publishRouter)
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp)),
      timestamp
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("tags" -> 1))
  }

  test("processDatapoints purged query") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(
        Map(
          "AWS/UT1" -> Map(
            "SumRate" ->
              (category.copy(filter = Some(Query.HasKey("MyTag"))), List.empty)
          )
        )
      )
    processor =
      new LocalCloudWatchMetricsProcessor(config, registry, ruleSpy, tagger, publishRouter)
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp)),
      timestamp
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("query" -> 1))
  }

  test("processDatapoints future data point") {
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp + 60_000)),
      timestamp
    )

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.sumrate",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "rate"
          ),
          timestamp,
          0.65
        )
      )
    )
    assertCounters(1, publishFuture = 1)
  }

  test("normalize") {
    assertEquals(1677706140000L, normalize(1677706164123L, 60))
    assertEquals(1677705900000L, normalize(1677706164123L, 300))
    assertEquals(1677704400000L, normalize(1677706164123L, 3600))
  }

  test("newCacheEntry") {
    assertEquals(cwDP.getNamespace, "AWS/UT1")
    assertEquals(cwDP.getMetric, "SumRate")
    assertEquals(cwDP.getDimensionsCount, 1)
    assertEquals(
      cwDP.getDimensions(0),
      CloudWatchDimension
        .newBuilder()
        .setName("MyTag")
        .setValue("Val")
        .build()
    )
    assertEquals(cwDP.getDataCount, 1)
    assertCWDP(cwDP.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertEquals(cwDP.getUnit, "Count")
  }

  test("newValue") {
    val nv = newValue(
      timestamp,
      Datapoint
        .builder()
        .sum(39.0)
        .minimum(1.0)
        .maximum(7.0)
        .sampleCount(19)
        .build()
    )
    assertCWDP(nv, timestamp, Array(39.0, 1.0, 7.0, 19))
  }

  test("insertDatapoint empty") {
    val updated = processor.insertDatapoint(
      cwDP.toBuilder.clearData().build().toByteArray,
      makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp),
      category,
      0
    )
    assertEquals(updated.getDataCount, 1)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCounters(0)
  }

  test("insertDatapoint after") {
    val updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp + 60_000),
      category,
      0
    )
    assertEquals(updated.getDataCount, 2)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCWDP(updated.getData(1), timestamp + 60_000, Array(80.0, 2.0, 6.0, 5))
    assertCounters(0)
  }

  test("insertDatapoint before") {
    val updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp - 60_000),
      category,
      0
    )
    assertEquals(updated.getDataCount, 2)
    assertCWDP(updated.getData(0), timestamp - 60_000, Array(80.0, 2.0, 6.0, 5))
    assertCWDP(updated.getData(1), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCounters(0, ooo = 1)
  }

  test("insertDatapoint between") {
    var updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp + (60_000 * 2)),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(2.0, 0.0, 1.0, 2), timestamp + 60_000),
      category,
      0
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCWDP(updated.getData(1), timestamp + 60_000, Array(2.0, 0.0, 1.0, 2))
    assertCWDP(updated.getData(2), timestamp + (60_000 * 2), Array(80.0, 2.0, 6.0, 5))
    assertCounters(0, ooo = 1)
  }

  test("insertDatapoint same first") {
    var updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(2.0, 0.0, 1.0, 2), timestamp + 60_000),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp + (60_000 * 2)),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(-1.0, -1.0, -1.0, -2), timestamp),
      category,
      0
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), timestamp, Array(-1.0, -1.0, -1.0, -2))
    assertCWDP(updated.getData(1), timestamp + 60_000, Array(2.0, 0.0, 1.0, 2))
    assertCWDP(updated.getData(2), timestamp + (60_000 * 2), Array(80.0, 2.0, 6.0, 5))
    assertCounters(0, dupes = 1)
  }

  test("insertDatapoint same middle") {
    var updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(2.0, 0.0, 1.0, 2), timestamp + 60_000),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp + (60_000 * 2)),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(-1.0, -1.0, -1.0, -2), timestamp + 60_000),
      category,
      0
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCWDP(updated.getData(1), timestamp + 60_000, Array(-1.0, -1.0, -1.0, -2))
    assertCWDP(updated.getData(2), timestamp + (60_000 * 2), Array(80.0, 2.0, 6.0, 5))
    assertCounters(0, dupes = 1)
  }

  test("insertDatapoint same last") {
    var updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(2.0, 0.0, 1.0, 2), timestamp + 60_000),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp + (60_000 * 2)),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(-1.0, -1.0, -1.0, -2), timestamp + (60_000 * 2)),
      category,
      0
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCWDP(updated.getData(1), timestamp + 60_000, Array(2.0, 0.0, 1.0, 2))
    assertCWDP(updated.getData(2), timestamp + (60_000 * 2), Array(-1.0, -1.0, -1.0, -2))
    assertCounters(0, dupes = 1)
  }

  test("insertDatapoint after and keep previous") {
    val updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp + 60_000),
      category,
      timestamp + 60_000
    )
    assertEquals(updated.getDataCount, 2)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCWDP(updated.getData(1), timestamp + 60_000, Array(80.0, 2.0, 6.0, 5))
    assertCounters(0)
  }

  test("insertDatapoint data point too old") {
    val updated = processor.insertDatapoint(
      cwDP.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp - (60_000 * 2)),
      category,
      timestamp
    )
    assertEquals(updated.getDataCount, 1)
    assertCWDP(updated.getData(0), timestamp, Array(39.0, 1.0, 7.0, 19))
    assertCounters(0, droppedOld = 1)
  }

  test("insertDatapoint expire old") {
    var updated = processor.insertDatapoint(
      cwDP.toBuilder.clearData().build().toByteArray,
      makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp - (60_000 * 2)),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp),
      category,
      timestamp
    )
    assertEquals(updated.getDataCount, 1)
    assertCWDP(updated.getData(0), timestamp, Array(80.0, 2.0, 6.0, 5))
    assertCounters(0)
  }

  test("insertDatapoint data point too old and expire old data to empty array") {
    var updated = processor.insertDatapoint(
      cwDP.toBuilder.clearData().build().toByteArray,
      makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), timestamp - (60_000 * 2)),
      category,
      0
    )
    updated = processor.insertDatapoint(
      updated.toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), timestamp - (60_000 * 2)),
      category,
      timestamp
    )
    assertEquals(updated.getDataCount, 0)
    assertCounters(0, droppedOld = 1)
  }

  test("toAWSDatapoint") {
    assertAWSDP(
      toAWSDatapoint(cwDP.getData(0), cwDP.getUnit),
      Array(39.0, 1.0, 7.0, 19),
      timestamp,
      "Count"
    )
  }

  test("toAWSDimensions") {
    val dimensions = toAWSDimensions(cwDP)
    assertEquals(dimensions.size, cwDP.getDimensionsCount)
    assertEquals(dimensions(0).name(), cwDP.getDimensions(0).getName)
    assertEquals(dimensions(0).value(), cwDP.getDimensions(0).getValue)
  }

  def assertPublished(dps: List[AtlasDatapoint], ts: Long = timestamp): Unit = {
    processor.publish(ts)
    verify(publishRouter, times(dps.size)).publish(routerCaptor)
    assertEquals(routerCaptor.values.size, dps.size)

    dps.foreach { dp =>
      val metric = routerCaptor.values.filter(_.tags.equals(dp.tags)).headOption match {
        case Some(d) => d
        case None =>
          throw new AssertionError(s"Data point not found: ${dp} in ${routerCaptor.values}")
      }
      assertEquals(metric.value, dp.value, s"Wrong value for ${dp.tags}")
      assertEquals(metric.timestamp, dp.timestamp, s"Wrong value for timestamp ${dp.tags}")
    }
  }

  def assertCounters(
    received: Long,
    filtered: Map[String, Long] = Map.empty,
    dupes: Long = 0,
    droppedOld: Long = 0,
    ooo: Long = 0,
    purged: Map[String, Long] = Map.empty,
    publishEmpty: Long = 0,
    publishFuture: Long = 0
  ): Unit = {
    assertEquals(registry.counter("atlas.cloudwatch.datapoints.received").count(), received)
    List("namespace", "metric", "tags", "query").foreach { reason =>
      assertEquals(
        registry.counter("atlas.cloudwatch.datapoints.filtered", "reason", reason).count(),
        filtered.getOrElse(reason, 0L),
        s"Count differs for ${reason}"
      )
    }

    assertEquals(
      registry.counter("atlas.cloudwatch.datapoints.dupes", "namespace", "AWS/DynamoDB").count(),
      dupes
    )
    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.datapoints.dropped",
          "reason",
          "tooOld",
          "namespace",
          "AWS/DynamoDB"
        )
        .count(),
      droppedOld
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.datapoints.ooo", "namespace", "AWS/DynamoDB").count(),
      ooo
    )

    List("namespace", "metric", "tags", "query").foreach { reason =>
      assertEquals(
        registry.counter("atlas.cloudwatch.datapoints.purged", "reason", reason).count(),
        purged.getOrElse(reason, 0L),
        s"Count differs for ${reason}"
      )
    }
    assertEquals(
      registry.counter("atlas.cloudwatch.publish.empty", "namespace", "AWS/UT1").count(),
      publishEmpty
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.publish.future", "namespace", "AWS/UT1").count(),
      publishFuture
    )
  }

}

object CloudWatchMetricsProcessorSuite {

  val timestamp = 1672531200000L

  def assertCWDP(cwdp: CloudWatchValue, timestamp: Long, values: Array[Double]): Unit = {
    assertEquals(cwdp.getTimestamp, timestamp)
    assertEquals(cwdp.getSum, values(0))
    assertEquals(cwdp.getMin, values(1))
    assertEquals(cwdp.getMax, values(2))
    assertEquals(cwdp.getCount, values(3))
  }

  def assertAWSDP(dp: Datapoint, values: Array[Double], ts: Long, unit: String): Unit = {
    assertEquals(dp.sum().asInstanceOf[Double], values(0))
    assertEquals(dp.minimum().asInstanceOf[Double], values(1))
    assertEquals(dp.maximum().asInstanceOf[Double], values(2))
    assertEquals(dp.sampleCount().asInstanceOf[Double], values(3))
    assertEquals(dp.timestamp().toEpochMilli, ts)
    assertEquals(dp.unit().toString, unit)
  }

  def makeFirehoseMetric(values: Array[Double], ts: Long): FirehoseMetric = {
    makeFirehoseMetric(
      "AWS/UT1",
      "SumRate",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      values,
      "Count",
      ts
    )
  }

  def makeFirehoseMetric(
    ns: String,
    metric: String,
    dimensions: List[Dimension],
    values: Array[Double],
    unit: String,
    ts: Long = timestamp
  ): FirehoseMetric = {
    FirehoseMetric(
      "unitTest",
      ns,
      metric,
      dimensions,
      Datapoint
        .builder()
        .timestamp(Instant.ofEpochMilli(ts))
        .sum(values(0))
        .minimum(values(1))
        .maximum(values(2))
        .sampleCount(values(3))
        .unit(unit)
        .build()
    )
  }

  def makeDatapoint(
    values: Array[Double],
    ts: Long = timestamp,
    unit: String = "Rate"
  ): Datapoint = {
    Datapoint
      .builder()
      .timestamp(Instant.ofEpochMilli(ts))
      .sum(values(0))
      .minimum(values(1))
      .maximum(values(2))
      .sampleCount(values(3))
      .unit(unit)
      .build()
  }

  case class CWDP(
    metric: String,
    values: Array[Double],
    dimensions: Int = 3,
    wTimestamp: Boolean = true
  ) {

    def encode(json: JsonGenerator): Unit = {
      json.writeStartObject()
      json.writeStringField("metric_stream_name", "Stream1")
      for (i <- 0 until Math.min(dimensions, 2)) {
        if (i == 0) json.writeStringField("account_id", "1234")
        if (i == 1) json.writeStringField("region", "us-west-2")
      }
      json.writeStringField("namespace", "UT/Test")
      if (metric != null) {
        json.writeStringField("metric_name", metric)
      }
      json.writeStringField("unit", "None")
      if (wTimestamp) {
        json.writeNumberField("timestamp", timestamp)
      }
      if (dimensions >= 3) {
        json.writeObjectFieldStart("dimensions")
        json.writeStringField("AwsTag", "AwsVal")
        json.writeEndObject()
      }
      if (values != null) {
        json.writeObjectFieldStart("value")
        for (i <- 0 until values.length) {
          if (i == 0) json.writeNumberField("sum", values(i))
          if (i == 1) json.writeNumberField("min", values(i))
          if (i == 2) json.writeNumberField("max", values(i))
          if (i == 3) json.writeNumberField("count", values(i))
          if (i > 3) json.writeNumberField(s"val${i}", values(i))
        }
        json.writeEndObject()
      }
      json.writeEndObject()
    }

  }

}
