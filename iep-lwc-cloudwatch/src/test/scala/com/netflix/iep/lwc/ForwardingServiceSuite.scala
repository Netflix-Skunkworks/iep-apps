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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.amazonaws.services.cloudwatch.model.Dimension
import com.amazonaws.services.cloudwatch.model.MetricDatum
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult
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
    val sample = s"""cluster->{"version":$versionWithUser,"payload":$payload}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse(
      ConfigBinVersion(1505226236957L, "2d4d", Some("brh"), Some("update")),
      ClusterConfig("bob@example.com", List(
        ForwardingExpression(
          atlasUri = expr,
          account = "$(nf.account)",
          metricName = "requestsPerSecond",
          dimensions = List(
            ForwardingDimension("AutoScalingGroupName", "$(nf.asg)")
          ))
      ))
    )

    assert(msg.cluster === "cluster")
    assert(msg.response.isUpdate)
    assert(msg.response === expectedResponse)
  }

  test("parse delete response") {
    val sample = s"""cluster->{"version":$version,"payload":""}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse.delete(ConfigBinVersion(1505226236957L, "2d4d"))

    assert(msg.cluster === "cluster")
    assert(msg.response.isDelete)
    assert(msg.response === expectedResponse)
  }

  test("parse heartbeat") {
    val sample = s"""heartbeat->1,i-12345,1a7fb69e-005c-4ebc-bf05-aba3386c682f,1"""
    val msg = Message(sample)

    assert(msg.cluster === "heartbeat")
    assert(msg.isHeartbeat)
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
  // toCloudWatchPut tests
  //

  def runCloudWatchPut(ns: String, vs: List[AccountDatum]): List[AccountRequest] = {
    val requests = List.newBuilder[AccountRequest]
    def doPut(account: String, request: PutMetricDataRequest): PutMetricDataResult = {
      requests += AccountRequest(account, request)
      new PutMetricDataResult
    }
    val future = Source(vs)
      .via(sendToCloudWatch(ns, doPut))
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
      AccountDatum("12345", datum)
    }
  }

  test("toCloudWatchPut: namespace") {
    val data = createDataSet(1)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = List(
      AccountRequest("12345", new PutMetricDataRequest()
        .withNamespace("Netflix/Namespace")
        .withMetricData(data.map(_.datum).asJava))
    )
    assert(actual === expected)
  }

  test("toCloudWatchPut: batching") {
    val data = createDataSet(20)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = List(
      AccountRequest("12345", new PutMetricDataRequest()
        .withNamespace("Netflix/Namespace")
        .withMetricData(data.map(_.datum).asJava))
    )
    assert(actual === expected)
  }

  test("toCloudWatchPut: multiple batches") {
    val data = createDataSet(27)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = data.grouped(20).toList.map { vs =>
      AccountRequest("12345", new PutMetricDataRequest()
        .withNamespace("Netflix/Namespace")
        .withMetricData(vs.map(_.datum).asJava))
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
      AccountDatum((i % mod).toString, datum)
    }
  }

  test("toCloudWatchPut: multiple accounts") {
    val data = createMultiAccountDataSet(5, 5)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = data.zipWithIndex.map {
      case (v, i) =>
        AccountRequest(i.toString, new PutMetricDataRequest()
          .withNamespace("Netflix/Namespace")
          .withMetricData(v.datum))
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
}