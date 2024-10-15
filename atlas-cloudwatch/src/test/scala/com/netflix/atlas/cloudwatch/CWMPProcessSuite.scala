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

import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.makeDatapoint
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.makeFirehoseMetric
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ts
import com.netflix.atlas.core.model.Query
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.spy
import org.mockito.MockitoSugar.times
import org.mockito.MockitoSugar.verify
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import scala.concurrent.duration.DurationInt

class CWMPProcessSuite extends BaseCloudWatchMetricsProcessorSuite {

  test("processDatapoints Empty") {
    val hash = makeFirehoseMetric(Array(1.0, 1.0, 1.0, 1), ts).xxHash
    processor
      .asInstanceOf[LocalCloudWatchMetricsProcessor]
      .inject(hash, cwDP.toBuilder.clearData().build().toByteArray, ts, 3600)

    assertPublished(List.empty, ts(10.minutes))
    assertCounters(0, publishEmpty = 1, scraped = 1)
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
      ts
    )
    assertPublished(List.empty)
    assertCounters(
      1,
      filtered = Map("namespace" -> (1, "AWS/SomeNoneExistingNS"), "metric" -> (0, "MyMetric"))
    )
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
      ts
    )
    assertPublished(List.empty)
    assertCounters(
      1,
      filtered = Map("namespace" -> (0, "AWS/UT1"), "metric" -> (1, "SomeUnknownMetric"))
    )
  }

  test("processDatapoints polling offset defined") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "DailyMetricA",
          List(Dimension.builder().name("MyTag").value("a").build()),
          Array(24, 0, 3, 10),
          "Count"
        )
      ),
      ts
    )
    assertPublished(List.empty)
    assertCounters(
      1,
      filtered = Map("namespace" -> (0, "AWS/UT1"), "polling" -> (1, "DailyMetricA"))
    )
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
      ts
    )
    assertPublished(List.empty)
    assertCounters(1, filtered = Map("namespace" -> (0, "AWS/UT1"), "tags" -> (1, "SumRate")))
  }

  test("processDatapoints match namespace without details") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/DetailsMeMaybe",
          "MyTestMetric",
          List(Dimension.builder().name("Key").value("Value").build()),
          Array(39.0, 1.0, 7.0, 19),
          "Sum"
        )
      ),
      ts
    )
    assertPublished(List.empty)
    assertCounters(1)
  }

  test("processDatapoints match namespace with details") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/DetailsMeMaybe",
          "MyTestMetric",
          List(
            Dimension.builder().name("Key").value("Value").name("Details").value("Test").build()
          ),
          Array(39.0, 1.0, 7.0, 19),
          "Sum"
        )
      ),
      ts
    )
    assertPublished(List.empty)
    assertCounters(1)
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
      ts
    )
    assertPublished(List.empty)
    assertCounters(1, filtered = Map("query" -> (1, "AWS/UTQueryFilter")))
  }

  test("processDatapoints matched dist-summary") {
    val dp = makeFirehoseMetric(
      "AWS/UT1",
      "Dist",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      Array(39.0, 1.0, 7.0, 19),
      "Count",
      ts(-1.minute)
    )
    processor.processDatapoints(List(dp), ts(-1.minute))

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
          ts,
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
          ts,
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
          ts,
          7.0
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints matched timer") {
    val dp = makeFirehoseMetric(
      "AWS/UT1",
      "Timer",
      List(
        Dimension.builder().name("LoadBalancerName").value("some-elb").build(),
        Dimension.builder().name("AvailabilityZone").value("a").build()
      ),
      Array(0.1226732730865479, 0.1226732730865479, 0.1226732730865479, 1),
      "Timer",
      ts(-1.minute)
    )
    processor.processDatapoints(List(dp), ts(-1.minute))

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
          ts,
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
          ts,
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
          ts,
          0.1226732730865479
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints matched bytes") {
    val dp = makeFirehoseMetric(
      "AWS/UT1",
      "Bytes",
      List(
        Dimension.builder().name("LoadBalancerName").value("some-elb").build(),
        Dimension.builder().name("AvailabilityZone").value("a").build()
      ),
      Array(1.6903156e7, 8378272.0, 8524884.0, 2),
      "Bytes",
      ts(-1.minute)
    )
    processor.processDatapoints(List(dp), ts(-1.minute))

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
          ts,
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
          ts(-1.minute)
        )
      ),
      ts
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
          ts,
          4.1320470343685605
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints config with end period offset") {
    // 1 end period offset so we back date.
    val dp = makeFirehoseMetric(
      "AWS/UT1",
      "Offset",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      Array(1, 1, 1, 1),
      "None",
      ts
    )
    processor.processDatapoints(List(dp), ts)
    processor.processDatapoints(
      List(
        dp.copy(
          datapoint = makeDatapoint(Array(2, 2, 2, 1), ts(1.minute), "None")
        )
      ),
      ts(1.minute)
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
          ts(1.minute),
          1
        )
      ),
      ts(1.minute)
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
      ts(-2.minute)
    )
    processor.processDatapoints(List(dp), ts(-2.minute))

    dp = dp.copy(datapoint = makeDatapoint(Array(120, 120, 120, 1), ts(-1.minute)))
    processor.processDatapoints(List(dp), ts(-1.minute))

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.mono",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "rate"
          ),
          ts,
          1.0
        )
      )
    )
    assertCounters(2)
  }

  test("processDatapoints rate") {
    val dp = makeFirehoseMetric(
      "AWS/UT1",
      "5Min",
      List(Dimension.builder().name("MyTag").value("Val").build()),
      Array(1, 1, 1, 1),
      "None",
      ts(-5.minute)
    )
    processor.processDatapoints(List(dp), ts(-5.minute))

    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.5min",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "rate"
          ),
          ts,
          0.0033333333333333335
        )
      )
    )
    assertCounters(1)
  }

  test("processDatapoints purged namespace") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(Map.empty)
    processor = new LocalCloudWatchMetricsProcessor(
      config,
      registry,
      ruleSpy,
      tagger,
      publishRouter,
      debugger
    )
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts)),
      ts
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("namespace" -> 1), scraped = 1)
  }

  test("processDatapoints purged metric") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(Map("AWS/UT1" -> Map("SomeMetric" -> List((category, List.empty)))))
    processor = new LocalCloudWatchMetricsProcessor(
      config,
      registry,
      ruleSpy,
      tagger,
      publishRouter,
      debugger
    )
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts)),
      ts
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("metric" -> 1), scraped = 1)
  }

  test("processDatapoints purged tags") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(
        Map(
          "AWS/UT1" -> Map(
            "SumRate" ->
              List((category.copy(dimensions = List("SomeOtherTag")), List.empty))
          )
        )
      )
    processor = new LocalCloudWatchMetricsProcessor(
      config,
      registry,
      ruleSpy,
      tagger,
      publishRouter,
      debugger
    )
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts)),
      ts
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("tags" -> 1), scraped = 1)
  }

  test("processDatapoints purged query") {
    val ruleSpy = spy(rules)
    when(ruleSpy.rules)
      .thenCallRealMethod()
      .thenReturn(
        Map(
          "AWS/UT1" -> Map(
            "SumRate" ->
              List((category.copy(filter = Some(Query.HasKey("MyTag"))), List.empty))
          )
        )
      )
    processor = new LocalCloudWatchMetricsProcessor(
      config,
      registry,
      ruleSpy,
      tagger,
      publishRouter,
      debugger
    )
    processor.processDatapoints(
      List(makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts)),
      ts
    )

    assertPublished(List.empty)
    assertCounters(1, purged = Map("query" -> 1), scraped = 1)
  }

  test("processDatapoints 2 rules match") {
    processor.processDatapoints(
      List(
        makeFirehoseMetric(
          "AWS/UT1",
          "TwoRuleMatch",
          List(Dimension.builder().name("MyTag").value("Val").build()),
          Array(39.0, 1.0, 7.0, 19),
          "None",
          ts
        )
      ),
      ts
    )

    // yup, two values with the same tag set. Just the min and max values.
    assertPublished(
      List(
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.2rulematch",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "gauge"
          ),
          ts,
          1
        ),
        com.netflix.atlas.core.model.Datapoint(
          Map(
            "name"         -> "aws.utm.2rulematch",
            "nf.region"    -> "us-west-2",
            "aws.tag"      -> "Val",
            "atlas.dstype" -> "gauge"
          ),
          ts,
          7
        )
      )
    )
    assertCounters(1)
  }

  def assertPublished(dps: List[AtlasDatapoint], ts: Long = ts): Unit = {
    processor.publish(ts)
    verify(publishRouter, times(dps.size)).publish(routerCaptor)
    assertEquals(routerCaptor.values.size, dps.size)

    dps.foreach { dp =>
      val metric = routerCaptor.values.filter(_.equals(dp)).headOption match {
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
    filtered: Map[String, (Long, String)] = Map.empty,
    droppedOld: Long = 0,
    purged: Map[String, Long] = Map.empty,
    publishEmpty: Long = 0,
    updates: Long = 0,
    scraped: Long = 0
  ): Unit = {
    assertEquals(processor.received.count(), received)
    List("namespace", "query").foreach { reason =>
      val (count, ns) = filtered.getOrElse(reason, (0L, "NA"))
      assertEquals(
        registry
          .counter("atlas.cloudwatch.datapoints.filtered", "aws.namespace", ns, "reason", reason)
          .count(),
        count,
        s"Count differs for ${reason}"
      )
    }

    List("metric", "tags").foreach { reason =>
      val (nsCount, ns) = filtered.getOrElse("namespace", (0L, "NA"))
      val (metricCount, metric) = filtered.getOrElse(reason, (0L, "NA"))
      assertEquals(
        registry
          .counter(
            "atlas.cloudwatch.datapoints.filtered",
            "aws.metric",
            metric,
            "aws.namespace",
            ns,
            "reason",
            reason
          )
          .count(),
        metricCount,
        s"Count differs for ${reason}"
      )
    }

    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.datapoints.dropped",
          "reason",
          "tooOld",
          "aws.namespace",
          "AWS/DynamoDB",
          "aws.metric",
          "SumRate"
        )
        .count(),
      droppedOld
    )

    List("namespace", "metric", "tags", "query").foreach { reason =>
      assertEquals(
        registry.counter("atlas.cloudwatch.datapoints.purged", "reason", reason).count(),
        purged.getOrElse(reason, 0L),
        s"Count differs for ${reason}"
      )
    }
    assertEquals(
      registry
        .counter(
          processor.publishEmpty.withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate")
        )
        .count(),
      publishEmpty
    )
    assertEquals(
      registry
        .counter(
          processor.cacheUpdates.withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate")
        )
        .count(),
      updates
    )
    assertEquals(
      registry
        .counter(
          processor.scraped.withTags("aws.namespace", "AWS/UT1", "aws.metric", "SumRate")
        )
        .count(),
      scraped
    )
  }
}
