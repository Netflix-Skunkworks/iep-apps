/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.iep.lwc.fwd.admin
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ScalingPoliciesDaoSuite extends FunSuite {
  val config = ConfigFactory.load()
  private implicit val system = ActorSystem()
  private implicit val mat = ActorMaterializer()

  val ec2PoliciesUri = config.getString("iep.lwc.fwding-admin.ec2-policies-uri")
  val cwAlarmsUri = config.getString("iep.lwc.fwding-admin.cw-alarms-uri")
  val titusPoliciesUri = config.getString("iep.lwc.fwding-admin.titus-policies-uri")

  test("Lookup all EC2 scaling policies") {
    val data = Map(
      Uri(ec2PoliciesUri) ->
      """
          |[
          |  {
          |    "alarms": [
          |      {
          |        "alarmName": "alarm1"
          |      }
          |    ],
          |    "policyName": "ec2Policy1"
          |  }
          |]
        """.stripMargin,
      Uri(cwAlarmsUri) ->
      """
          |[
          |  {
          |    "alarmName": "alarm1",
          |    "metricName": "metric1",
          |    "dimensions": [
          |      {
          |        "value": "asg1",
          |        "name": "AutoScalingGroupName"
          |      }
          |    ]
          |  }
          |]
        """.stripMargin,
      Uri(titusPoliciesUri) -> "[]"
    )

    val dao = new LocalScalingPoliciesDaoTestImpl(data, config, system)
    val future = getScalingPolicies(dao, EddaEndpoint("123", "us-east-1", "test"))

    val actual = Await.result(future, Duration.Inf)
    val expected = List(
      ScalingPolicy(
        "ec2Policy1",
        ScalingPolicy.Ec2,
        "metric1",
        List(MetricDimension("AutoScalingGroupName", "asg1"))
      )
    )
    assert(actual === expected)
  }

  test("Lookup Titus scaling policies of type Target tracking") {
    val data = Map(
      Uri(ec2PoliciesUri) -> "[]",
      Uri(cwAlarmsUri)    -> "[]",
      Uri(titusPoliciesUri) ->
      """
          |[
          |  {
          |    "id": {
          |      "id": "titusPolicy1"
          |    },
          |    "scalingPolicy": {
          |      "targetPolicyDescriptor": {
          |        "customizedMetricSpecification": {
          |          "dimensions": [
          |            {
          |              "value": "asg1",
          |              "name": "AutoScalingGroupName"
          |            }
          |          ],
          |          "metricName": "metric1"
          |        }
          |      }
          |    }
          |  }
          |]
        """.stripMargin
    )

    val dao = new LocalScalingPoliciesDaoTestImpl(data, config, system)
    val future = getScalingPolicies(dao, EddaEndpoint("123", "us-east-1", "test"))

    val actual = Await.result(future, Duration.Inf)
    val expected = List(
      ScalingPolicy(
        "titusPolicy1",
        ScalingPolicy.Titus,
        "metric1",
        List(MetricDimension("AutoScalingGroupName", "asg1"))
      )
    )
    assert(actual === expected)
  }

  test("Lookup Titus scaling policies of type Step scaling") {
    val data = Map(
      Uri(ec2PoliciesUri) -> "[]",
      Uri(cwAlarmsUri) ->
      """
          |[
          |  {
          |    "alarmName": "job1/titusPolicy1",
          |    "metricName": "metric1",
          |    "dimensions": [
          |      {
          |        "value": "asg1",
          |        "name": "AutoScalingGroupName"
          |      }
          |    ]
          |  }
          |]
        """.stripMargin,
      Uri(titusPoliciesUri) ->
      """
        |[
        |  {
        |    "jobId": "job1",
        |    "id": {
        |      "id": "titusPolicy1"
        |    },
        |    "scalingPolicy": {
        |      "stepPolicyDescriptor": {}
        |    }
        |  }
        |]
      """.stripMargin
    )

    val dao = new LocalScalingPoliciesDaoTestImpl(data, config, system)
    val future = getScalingPolicies(dao, EddaEndpoint("123", "us-east-1", "test"))

    val actual = Await.result(future, Duration.Inf)
    val expected = List(
      ScalingPolicy(
        "titusPolicy1",
        ScalingPolicy.Titus,
        "metric1",
        List(MetricDimension("AutoScalingGroupName", "asg1"))
      )
    )
    assert(actual === expected)
  }

  test("Fail when a downstream call fails") {
    // No data for titusPoliciesUri should trigger a failure
    val data = Map(
      Uri(ec2PoliciesUri) -> "[]",
      Uri(cwAlarmsUri)    -> "[]"
    )

    val dao = new LocalScalingPoliciesDaoTestImpl(data, config, system)
    val future = getScalingPolicies(dao, EddaEndpoint("123", "us-east-1", "test"))

    assertThrows[NoSuchElementException](Await.result(future, Duration.Inf))
  }

  test("Fail when status is not ok") {
    val dao = new LocalScalingPoliciesDaoTestImpl(Map.empty[Uri, String], config, system) {
      override def makeResponse(uri: Uri): Try[HttpResponse] = {
        Success(HttpResponse(StatusCodes.InternalServerError))
      }
    }
    val future = getScalingPolicies(dao, EddaEndpoint("123", "us-east-1", "test"))

    assertThrows[NoSuchElementException](Await.result(future, Duration.Inf))
  }

  def getScalingPolicies(
    dao: ScalingPoliciesDao,
    eddaEndpoint: EddaEndpoint
  ): Future[List[ScalingPolicy]] = {
    Source
      .single(EddaEndpoint("123", "us-east-1", "test"))
      .via(dao.getScalingPolicies())
      .runWith(Sink.head)
  }
}

class LocalScalingPoliciesDaoTestImpl(
  data: Map[Uri, String],
  config: Config,
  override implicit val system: ActorSystem
) extends ScalingPoliciesDaoImpl(config, system) {
  override protected val client = Flow[(HttpRequest, AccessLogger)]
    .map {
      case (request, accessLogger) =>
        (
          makeResponse(request.uri),
          accessLogger
        )
    }

  def makeResponse(uri: Uri): Try[HttpResponse] = {
    data
      .get(uri)
      .map { body =>
        Success(
          HttpResponse(
            StatusCodes.OK,
            entity = HttpEntity(MediaTypes.`application/json`, body)
          )
        )
      }
      .getOrElse {
        Failure(new RuntimeException("Error fetching data"))
      }
  }
}
