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

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.makeFirehoseMetric
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.nts
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ts
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newCacheEntry
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.Assertions.assertEquals
import munit.FunSuite
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.ImplicitSender
import org.apache.pekko.testkit.TestKitBase
import org.junit.Ignore
import org.mockito.MockitoSugar.mock
import org.mockito.captor.ArgCaptor
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import java.time.Instant
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IterableHasAsJava

@Ignore
class BaseCloudWatchMetricsProcessorSuite extends FunSuite with TestKitBase with ImplicitSender {

  override implicit def system: ActorSystem = ActorSystem("Test")

  var registry: Registry = null
  var publishRouter: PublishRouter = null
  var processor: CloudWatchMetricsProcessor = null
  var routerCaptor = ArgCaptor[AtlasDatapoint]
  var debugger: CloudWatchDebugger = null
  val config = ConfigFactory.load()
  val tagger = new NetflixTagger(config.getConfig("atlas.cloudwatch.tagger"))
  val rules: CloudWatchRules = new CloudWatchRules(config)
  val category = MetricCategory("AWS/DynamoDB", 60, -1, List("MyTag"), null, List.empty, null)
  val category5m = MetricCategory("AWS/DynamoDB", 300, -1, List("MyTag"), null, List.empty, null)

  val cwDP = newCacheEntry(
    makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts(-2.minutes)),
    category,
    nts
  )

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    publishRouter = mock[PublishRouter]
    debugger = new CloudWatchDebugger(config, registry)
    processor =
      new LocalCloudWatchMetricsProcessor(config, registry, rules, tagger, publishRouter, debugger)
    routerCaptor = ArgCaptor[AtlasDatapoint]
  }

}

object BaseCloudWatchMetricsProcessorSuite {

  val ts = 1672531745000L

  /** To the next minute */
  val nts = 1672531800000L

  def ts(duration: Duration): Long = ts + duration.toMillis

  def nts(duration: Duration): Long = nts + duration.toMillis

  def offset(duration: Duration): Long = duration.toMillis

  def ce(
    entries: Seq[CloudWatchValue]
  ): CloudWatchCacheEntry = {
    val builder = CloudWatchCacheEntry
      .newBuilder()
      .setMetric("SumRate")
      .setNamespace("AWS/UT1")
      .setUnit("Count")
      .addAllData(entries.toList.asJava)
    builder.build()
  }

  def cwv(
    timestamp: Duration,
    update: Duration,
    published: Boolean,
    values: Option[Array[Double]] = None
  ): CloudWatchValue = {
    val bldr = CloudWatchValue
      .newBuilder()
      .setTimestamp(ts(timestamp))
      .setUpdateTimestamp(nts(update))
      .setPublished(published)
    if (values.isDefined) {
      val v = values.get
      bldr.setSum(v(0))
      bldr.setMin(v(1))
      bldr.setMax(v(2))
      bldr.setCount(v(3))
    } else {
      bldr.setSum(1.0)
      bldr.setMin(1.0)
      bldr.setMax(1.0)
      bldr.setCount(1.0)
    }
    bldr.build()
  }

  def assertCWDP(
    cwdp: CloudWatchValue,
    timestamp: Long,
    values: Array[Double],
    updateTimestamp: Long = nts
  ): Unit = {
    assertEquals(cwdp.getTimestamp, timestamp)
    assertEquals(cwdp.getUpdateTimestamp, updateTimestamp)
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
    ts: Long = ts,
    streamName: String = "unitTest"
  ): FirehoseMetric = {
    FirehoseMetric(
      streamName,
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
    ts: Long = ts,
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
        json.writeNumberField("timestamp", ts)
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
