/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.iep.lwc

import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.amazonaws.services.cloudwatch.model.Dimension
import com.amazonaws.services.cloudwatch.model.MetricDatum
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.spectator.api.NoopRegistry
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ForwardingServiceSuite extends FunSuite {

  import ForwardingService._

  import scala.collection.JavaConverters._

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private val version =
    """{"ts":1505226236957,"hash":"2d4d"}"""

  private val versionWithUser =
    """{"ts":1505226236957,"hash":"2d4d","user":"brh","comment":"update"}"""

  private val expr =
    """name,http.req.complete,:eq,statistic,count,:eq,:and,:sum,(,nf.account,nf.asg,),:by"""

  private val payload =
    s"""{"email":"bob@example.com","expressions":[{"atlasUri":"$expr","account":"$$(nf.account)","metricName":"requestsPerSecond","dimensions":[{"name":"AutoScalingGroupName","value":"$$(nf.asg)"}]}]}"""

  test("parse update response") {
    val sample =
      s"""data: {"type":"config", "key":"cluster", "data":{"version":$versionWithUser,"payload":$payload}}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse(
      ConfigBinVersion(1505226236957L, "2d4d", Some("brh"), Some("update")),
      ClusterConfig(
        "bob@example.com",
        List(
          ForwardingExpression(
            atlasUri = expr,
            account = "$(nf.account)",
            region = None,
            metricName = "requestsPerSecond",
            dimensions = List(
              ForwardingDimension("AutoScalingGroupName", "$(nf.asg)")
            )
          )
        )
      )
    )

    assert(!msg.isInvalid)
    assert(msg.isUpdate)
    assert(!msg.isDone)
    assert(!msg.isHeartbeat)
    assert(msg.cluster === "cluster")
    assert(msg.response.isUpdate)
    assert(msg.response === expectedResponse)
  }

  test("parse delete response") {
    val sample =
      s"""data: {"type":"config", "key":"cluster", "data":{"version":$version,"payload":""}}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse.delete(ConfigBinVersion(1505226236957L, "2d4d"))

    assert(msg.cluster === "cluster")
    assert(msg.response.isDelete)
    assert(msg.response === expectedResponse)
  }

  test("parse heartbeat") {
    val sample =
      s"""data: {"type":"heartbeat","key":"heartbeat","data":{"clientRepoVersion":1,"serverInstance":"i-12345","clientId":"1a7fb69e-005c-4ebc-bf05-aba3386c682f","latestVersion":1}}"""
    val msg = Message(sample)

    assert(!msg.isInvalid)
    assert(!msg.isUpdate)
    assert(!msg.isDone)
    assert(msg.isHeartbeat)
  }

  test("parse done") {
    val sample = s"""data: {"type":"done","key":"","data":{}}"""
    val msg = Message(sample)

    assert(!msg.isInvalid)
    assert(!msg.isUpdate)
    assert(msg.isDone)
    assert(!msg.isHeartbeat)
  }

  test("invalid config message") {
    val sample = s"""data: {"type":"config","key":"cluster","data":{}}"""
    val msg = Message(sample)

    assert(msg.isInvalid)
    assert(!msg.isUpdate)
  }

  test("invalid heartbeat message") {
    val sample = s"""data: {"type":"heartbeat","key":"heartbeat","data":{}}"""
    val msg = Message(sample)

    assert(msg.isInvalid)
    assert(msg.isHeartbeat)
  }

  test("invalid sse message") {
    val sample = s"""unknown: {"type":"config","key":"cluster","data":{}}"""
    val msg = Message(sample)

    assert(msg.isInvalid)
    assert(!msg.isUpdate)
  }

  //
  // CloudWatch Truncate tests
  //

  test("truncate: NaN") {
    assert(truncate(Double.NaN) === 0.0)
  }

  test("truncate: Infinity") {
    assert(truncate(Double.PositiveInfinity) === math.pow(2.0, 360))
  }

  test("truncate: -Infinity") {
    assert(truncate(Double.NegativeInfinity) === -math.pow(2.0, 360))
  }

  test("truncate: large value") {
    assert(truncate(math.pow(2.0, 400)) === math.pow(2.0, 360))
  }

  test("truncate: negative large value") {
    assert(truncate(-math.pow(2.0, 400)) === -math.pow(2.0, 360))
  }

  test("truncate: large negative exponent") {
    assert(truncate(math.pow(2.0, -400)) === 0.0)
  }

  //
  // toMetricDatum tests
  //

  def runToMetricDatum(env: Evaluator.MessageEnvelope): AccountDatum = {
    val future = Source
      .single(env)
      .via(toMetricDatum(new NoopRegistry))
      .runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("toMetricDatum: basic") {
    val msg = TimeSeriesMessage(
      id = "abc",
      query = "name,ssCpuUser,:eq,:sum",
      groupByKeys = Nil,
      start = 0L,
      end = 60000L,
      step = 60000L,
      label = "test",
      tags = Map("name" -> "ssCpuUser"),
      data = ArrayData(Array(1.0))
    )
    val id =
      """
        |{
        |  "atlasUri": "http://atlas/api/v1/graph?q=name,ssCpuUser,:eq,:sum",
        |  "account": "1234567890",
        |  "metricName": "ssCpuUser",
        |  "dimensions": []
        |}
      """.stripMargin
    val env = new Evaluator.MessageEnvelope(id, msg)
    val actual = runToMetricDatum(env)
    assert(actual.account === "1234567890")
    assert(actual.datum.getMetricName === "ssCpuUser")
  }

  //
  // sendToCloudWatch tests
  //

  def runCloudWatchPut(ns: String, vs: List[AccountDatum]): List[AccountRequest] = {
    val requests = List.newBuilder[AccountRequest]
    def doPut(
      region: String,
      account: String,
      request: PutMetricDataRequest
    ): PutMetricDataResult = {
      requests += AccountRequest(region, account, request)
      new PutMetricDataResult
    }
    val future = Source(vs)
      .via(sendToCloudWatch(new AtomicLong(), ns, doPut))
      .runWith(Sink.ignore)
    Await.result(future, Duration.Inf)
    requests.result()
  }

  def createDataSet(n: Int): List[AccountDatum] = {
    (0 until n).toList.map { i =>
      val datum = new MetricDatum()
        .withMetricName(i.toString)
        .withDimensions(new Dimension().withName("foo").withValue("bar"))
        .withTimestamp(new Date())
        .withValue(i.toDouble)
      AccountDatum("us-east-1", "12345", datum)
    }
  }

  test("toCloudWatchPut: namespace") {
    val data = createDataSet(1)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = List(
      AccountRequest(
        "us-east-1",
        "12345",
        new PutMetricDataRequest()
          .withNamespace("Netflix/Namespace")
          .withMetricData(data.map(_.datum).asJava)
      )
    )
    assert(actual === expected)
  }

  test("toCloudWatchPut: batching") {
    val data = createDataSet(20)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = List(
      AccountRequest(
        "us-east-1",
        "12345",
        new PutMetricDataRequest()
          .withNamespace("Netflix/Namespace")
          .withMetricData(data.map(_.datum).asJava)
      )
    )
    assert(actual === expected)
  }

  test("toCloudWatchPut: multiple batches") {
    val data = createDataSet(27)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = data.grouped(20).toList.map { vs =>
      AccountRequest(
        "us-east-1",
        "12345",
        new PutMetricDataRequest()
          .withNamespace("Netflix/Namespace")
          .withMetricData(vs.map(_.datum).asJava)
      )
    }
    assert(actual === expected)
  }

  def createMultiAccountDataSet(mod: Int, n: Int): List[AccountDatum] = {
    (0 until n).toList.map { i =>
      val datum = new MetricDatum()
        .withMetricName(i.toString)
        .withDimensions(new Dimension().withName("foo").withValue("bar"))
        .withTimestamp(new Date())
        .withValue(i.toDouble)
      AccountDatum("eu-west-1", (i % mod).toString, datum)
    }
  }

  test("toCloudWatchPut: multiple accounts") {
    val data = createMultiAccountDataSet(5, 5)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = data.zipWithIndex.map {
      case (v, i) =>
        AccountRequest(
          "eu-west-1",
          i.toString,
          new PutMetricDataRequest()
            .withNamespace("Netflix/Namespace")
            .withMetricData(v.datum)
        )
    }
    assert(actual.sortWith(_.account < _.account) === expected)
  }

  test("toCloudWatchPut: multiple accounts and batching") {
    val data = createMultiAccountDataSet(2, 47)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    assert(actual.size === 4)
    assert(actual.map(_.account).toSet === Set("0", "1"))
    assert(actual.filter(_.account == "0").map(_.request.getMetricData.size()).sum === 24)
    assert(actual.filter(_.account == "1").map(_.request.getMetricData.size()).sum === 23)
  }

  def createMultiRegionDataSet(mod: Int, n: Int): List[AccountDatum] = {
    val regions = Array("us-east-1", "eu-west-1", "us-west-2")
    (0 until n).toList.map { i =>
      val datum = new MetricDatum()
        .withMetricName(i.toString)
        .withDimensions(new Dimension().withName("foo").withValue("bar"))
        .withTimestamp(new Date())
        .withValue(i.toDouble)
      AccountDatum(regions(i % regions.length), (i % mod).toString, datum)
    }
  }

  test("toCloudWatchPut: multiple regions") {
    val data = createMultiAccountDataSet(5, 5)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = data.map { v =>
      AccountRequest(
        v.region,
        v.account,
        new PutMetricDataRequest()
          .withNamespace("Netflix/Namespace")
          .withMetricData(v.datum)
      )
    }
    assert(actual.sortWith(_.account < _.account) === expected)
  }
}
