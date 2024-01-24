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
package com.netflix.iep.lwc

import java.util.concurrent.atomic.AtomicLong
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.*
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse

import java.time.Instant
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Success

class ForwardingServiceSuite extends FunSuite {

  import ForwardingService.*

  import scala.jdk.CollectionConverters.*

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private val version =
    """{"ts":1505226236957,"hash":"2d4d"}"""

  private val versionWithUser =
    """{"ts":1505226236957,"hash":"2d4d","user":"brh","comment":"update"}"""

  private val expr =
    """name,http.req.complete,:eq,statistic,count,:eq,:and,:sum,(,nf.account,nf.asg,),:by"""

  private val payload =
    s"""
       |{
       |  "email":"bob@example.com",
       |  "expressions":[
       |    {
       |      "atlasUri":"$expr",
       |      "account":"$$(nf.account)",
       |      "metricName":"requestsPerSecond",
       |      "dimensions":[{"name":"AutoScalingGroupName","value":"$$(nf.asg)"}]
       |    }
       |  ],
       |  "checksToSkip": []
       |}""".stripMargin

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
    assertEquals(msg.cluster, "cluster")
    assert(msg.response.isUpdate)
    assertEquals(msg.response, expectedResponse)
  }

  test("parse delete response") {
    val sample =
      s"""data: {"type":"config", "key":"cluster", "data":{"version":$version,"payload":""}}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse.delete(ConfigBinVersion(1505226236957L, "2d4d"))

    assertEquals(msg.cluster, "cluster")
    assert(msg.response.isDelete)
    assertEquals(msg.response, expectedResponse)
  }

  test("parse heartbeat") {
    val sample =
      s"""data: {"type":"heartbeat","key":"heartbeat","data":{"clientRepoVersion":1,"serverInstance":"i-12345","clientId":"1a7fb69e-005c-4ebc-bf05-aba3386c682f","latestVersion":1}}"""
    val msg = Message(sample)

    assert(!msg.isInvalid)
    assert(!msg.isUpdate)
    assert(!msg.isDone)
    assert(msg.isHeartbeat)

    assertEquals(msg.repoVersion, 1L)
  }

  test("parse heartbeat with no repo version") {
    val sample =
      s"""data: {"type":"heartbeat","key":"heartbeat","data":{"serverInstance":"i-12345","clientId":"1a7fb69e-005c-4ebc-bf05-aba3386c682f","latestVersion":1}}"""
    val msg = Message(sample)

    assert(!msg.isInvalid)
    assert(!msg.isUpdate)
    assert(!msg.isDone)
    assert(msg.isHeartbeat)

    assertEquals(msg.repoVersion, -1L)
  }

  test("parse heartbeat with bad repo version type") {
    val sample =
      s"""data: {"type":"heartbeat","key":"heartbeat","data":{"clientRepoVersion":"bad","serverInstance":"i-12345","clientId":"1a7fb69e-005c-4ebc-bf05-aba3386c682f","latestVersion":1}}"""
    val msg = Message(sample)

    assert(!msg.isInvalid)
    assert(!msg.isUpdate)
    assert(!msg.isDone)
    assert(msg.isHeartbeat)

    assertEquals(msg.repoVersion, -1L)
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
    assertEquals(truncate(Double.NaN), 0.0)
  }

  test("truncate: Infinity") {
    assertEquals(truncate(Double.PositiveInfinity), math.pow(2.0, 360))
  }

  test("truncate: -Infinity") {
    assertEquals(truncate(Double.NegativeInfinity), -math.pow(2.0, 360))
  }

  test("truncate: large value") {
    assertEquals(truncate(math.pow(2.0, 400)), math.pow(2.0, 360))
  }

  test("truncate: negative large value") {
    assertEquals(truncate(-math.pow(2.0, 400)), -math.pow(2.0, 360))
  }

  test("truncate: large negative exponent") {
    assertEquals(truncate(math.pow(2.0, -400)), 0.0)
  }

  //
  // toMetricDatum tests
  //

  def runToMetricDatum(env: Evaluator.MessageEnvelope): ForwardingMsgEnvelope = {
    val future = Source
      .single(env)
      .via(toMetricDatum(ConfigFactory.load(), new NoopRegistry))
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
        |  "key": "cluster1",
        |  "expression": {
        |    "atlasUri": "http://atlas/api/v1/graph?q=name,ssCpuUser,:eq,:sum",
        |    "account": "1234567890",
        |    "metricName": "ssCpuUser",
        |    "dimensions": []
        |  }
        |}
      """.stripMargin
    val env = new Evaluator.MessageEnvelope(id, msg)
    val actual = runToMetricDatum(env)

    assertEquals(actual.id, Json.decode[ExpressionId](id))
    assertEquals(actual.accountDatum.get.account, "1234567890")
    assertEquals(actual.accountDatum.get.datum.metricName(), "ssCpuUser")
    assertEquals(actual.accountDatum.get.datum.storageResolution().toInt, 60)
  }

  test("toMetricDatum: filter out NaN values") {
    val msg = TimeSeriesMessage(
      id = "abc",
      query = "name,ssCpuUser,:eq,:sum",
      groupByKeys = Nil,
      start = 0L,
      end = 60000L,
      step = 60000L,
      label = "test",
      tags = Map("name" -> "ssCpuUser"),
      data = ArrayData(Array(Double.NaN))
    )
    val id =
      """
        |{
        |  "key": "cluster1",
        |  "expression": {
        |    "atlasUri": "http://atlas/api/v1/graph?q=name,ssCpuUser,:eq,:sum",
        |    "account": "$(nf.account)",
        |    "metricName": "ssCpuUser",
        |    "dimensions": []
        |  }
        |}
      """.stripMargin
    val env = new Evaluator.MessageEnvelope(id, msg)
    val actual = runToMetricDatum(env).accountDatum
    assertEquals(actual, None)
  }

  test("toMetricDatum: high resolution") {
    val msg = TimeSeriesMessage(
      id = "abc",
      query = "name,ssCpuUser,:eq,:sum",
      groupByKeys = Nil,
      start = 0L,
      end = 60000L,
      step = 5000L,
      label = "test",
      tags = Map("name" -> "ssCpuUser"),
      data = ArrayData(Array(1.0))
    )
    val id =
      """
        |{
        |  "key": "cluster1",
        |  "expression": {
        |    "atlasUri": "http://atlas/api/v1/graph?q=name,ssCpuUser,:eq,:sum",
        |    "account": "1234567890",
        |    "metricName": "ssCpuUser",
        |    "dimensions": []
        |  }
        |}
      """.stripMargin
    val env = new Evaluator.MessageEnvelope(id, msg)
    val actual = runToMetricDatum(env)

    assertEquals(actual.id, Json.decode[ExpressionId](id))
    assertEquals(actual.accountDatum.get.account, "1234567890")
    assertEquals(actual.accountDatum.get.datum.metricName(), "ssCpuUser")
    assertEquals(actual.accountDatum.get.datum.storageResolution().toInt, 1)
  }

  //
  // sendToCloudWatch tests
  //

  def runCloudWatchPut(ns: String, vs: List[AccountDatum]): List[AccountRequest] = {
    val id =
      ExpressionId(
        "",
        ForwardingExpression("", "", None, "", List.empty[ForwardingDimension])
      )

    val (requests, _) = doRunCloudWatchPut(
      ns,
      vs.map(a => ForwardingMsgEnvelope(id, Some(a), None))
    )
    requests
  }

  def doRunCloudWatchPut(
    ns: String,
    msgs: List[ForwardingMsgEnvelope]
  ): (List[AccountRequest], Seq[ForwardingMsgEnvelope]) = {
    val requests = List.newBuilder[AccountRequest]
    def doPut(
      region: String,
      account: String,
      request: PutMetricDataRequest
    ): PutMetricDataResponse = {
      requests += AccountRequest(region, account, request)
      PutMetricDataResponse.builder().build()
    }
    val future = Source(msgs)
      .via(sendToCloudWatch(new AtomicLong(), ns, doPut))
      .runWith(Sink.seq)
    val msgsOut = Await.result(future, Duration.Inf)

    (requests.result(), msgsOut)
  }

  def createDataSet(n: Int): List[AccountDatum] = {
    (0 until n).toList.map { i =>
      val datum = MetricDatum
        .builder()
        .metricName(i.toString)
        .dimensions(Dimension.builder().name("foo").value("bar").build())
        .timestamp(Instant.now())
        .value(i.toDouble)
        .build()
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
        PutMetricDataRequest
          .builder()
          .namespace("Netflix/Namespace")
          .metricData(data.map(_.datum).asJava)
          .build()
      )
    )
    assertEquals(actual, expected)
  }

  test("toCloudWatchPut: batching") {
    val data = createDataSet(20)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = List(
      AccountRequest(
        "us-east-1",
        "12345",
        PutMetricDataRequest
          .builder()
          .namespace("Netflix/Namespace")
          .metricData(data.map(_.datum).asJava)
          .build()
      )
    )
    assertEquals(actual, expected)
  }

  test("toCloudWatchPut: multiple batches") {
    val data = createDataSet(27)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    val expected = data.grouped(20).toList.map { vs =>
      AccountRequest(
        "us-east-1",
        "12345",
        PutMetricDataRequest
          .builder()
          .namespace("Netflix/Namespace")
          .metricData(vs.map(_.datum).asJava)
          .build()
      )
    }
    assertEquals(actual, expected)
  }

  test("toCloudWatchPut: skip put and pass the msg through for no data") {
    val id =
      ExpressionId(
        "",
        ForwardingExpression("", "", None, "", List.empty[ForwardingDimension])
      )
    val msgs = List(ForwardingMsgEnvelope(id, None, None))
    val (requests, msgsOut) = doRunCloudWatchPut("Netflix/Namespace", msgs)

    assert(requests.isEmpty)
    assertEquals(msgsOut.toList, msgs)
  }

  test("toCloudWatchPut: stream with metric and no data") {
    val ns = "Netflix/Namespace"
    val id =
      ExpressionId(
        "",
        ForwardingExpression("", "", None, "", List.empty[ForwardingDimension])
      )
    val dataMsgs = createDataSet(20).map(a => ForwardingMsgEnvelope(id, Some(a), None))
    val noDataMsgsIn = List.fill(20)(ForwardingMsgEnvelope(id, None, None))

    val (requests, msgsOut) = doRunCloudWatchPut(ns, dataMsgs ++ noDataMsgsIn)

    val expectedReqs = List(
      AccountRequest(
        "us-east-1",
        "12345",
        PutMetricDataRequest
          .builder()
          .namespace(ns)
          .metricData(dataMsgs.map(_.accountDatum.get.datum).asJava)
          .build()
      )
    )

    assertEquals(requests, expectedReqs)
    assertEquals(msgsOut.toSet, (noDataMsgsIn ++ dataMsgs).toSet)
  }

  def createMultiAccountDataSet(mod: Int, n: Int): List[AccountDatum] = {
    (0 until n).toList.map { i =>
      val datum = MetricDatum
        .builder()
        .metricName(i.toString)
        .dimensions(Dimension.builder().name("foo").value("bar").build())
        .timestamp(Instant.now())
        .value(i.toDouble)
        .build()
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
          PutMetricDataRequest
            .builder()
            .namespace("Netflix/Namespace")
            .metricData(v.datum)
            .build()
        )
    }
    assertEquals(actual.sortWith(_.account < _.account), expected)
  }

  test("toCloudWatchPut: multiple accounts and batching") {
    val data = createMultiAccountDataSet(2, 47)
    val actual = runCloudWatchPut("Netflix/Namespace", data)
    assertEquals(actual.size, 4)
    assertEquals(actual.map(_.account).toSet, Set("0", "1"))
    assertEquals(actual.filter(_.account == "0").map(_.request.metricData.size()).sum, 24)
    assertEquals(actual.filter(_.account == "1").map(_.request.metricData.size()).sum, 23)
  }

  def createMultiRegionDataSet(mod: Int, n: Int): List[AccountDatum] = {
    val regions = Array("us-east-1", "eu-west-1", "us-west-2")
    (0 until n).toList.map { i =>
      val datum = MetricDatum
        .builder()
        .metricName(i.toString)
        .dimensions(Dimension.builder().name("foo").value("bar").build())
        .timestamp(Instant.now())
        .value(i.toDouble)
        .build()
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
        PutMetricDataRequest
          .builder()
          .namespace("Netflix/Namespace")
          .metricData(v.datum)
          .build()
      )
    }
    assertEquals(actual.sortWith(_.account < _.account), expected)
  }

  //
  // sendToAdmin tests
  //

  test("Send the messages to Admin endpoint") {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    val requests = List.newBuilder[HttpRequest]
    val client: Client =
      Flow[(HttpRequest, AccessLogger)]
        .map {
          case (request, accessLogger) =>
            requests += request
            (Success(HttpResponse(StatusCodes.OK)), accessLogger)
        }

    val id =
      ExpressionId(
        "",
        ForwardingExpression("", "", None, "", List.empty[ForwardingDimension])
      )
    val msgs = createDataSet(1).map(a => ForwardingMsgEnvelope(id, Some(a), None))
    val future = Source(msgs)
      .via(sendToAdmin(Uri("http://local:7101/api/v1/report"), client))
      .runWith(Sink.head)

    val result = Await.result(future, Duration.Inf)

    val reqBodyFuture = Future
      .sequence(
        requests.result().map(Unmarshal(_).to[String])
      )
      .map(_.head)
      .map(Json.decode[Seq[Report]](_).head)

    val reqBody = Await.result(reqBodyFuture, Duration.Inf)

    val expectedReport = Report(
      1L,
      id,
      Some(FwdMetricInfo("us-east-1", "12345", "0", Map("foo" -> "bar"))),
      None
    )

    assertEquals(result, NotUsed)
    assertEquals(reqBody.copy(timestamp = 1L), expectedReport)

  }
}
