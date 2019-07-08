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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.netflix.iep.lwc.fwd.cw.Report
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MarkerServiceSuite extends FunSuite {

  import MarkerServiceImpl._

  private val config = ConfigFactory.load()
  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()
  implicit val ec = scala.concurrent.ExecutionContext.global

  test("Read ExpressionDetails using a dedicated dispatcher") {
    val data = ExpressionDetails(
      ExpressionId("", ForwardingExpression("", "", None, "", Nil)),
      0L,
      Nil,
      None,
      Map.empty[String, Long],
      Nil
    )
    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def read(id: ExpressionId): Option[ExpressionDetails] = {
        Some(data)
      }
    }

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )

    val future = Source
      .single(report)
      .via(readExprDetails(exprDetailsDao))
      .runWith(Sink.head)
    val actual = Await.result(future, Duration.Inf)
    assert(actual === Some(data))
  }

  test("Read errors should be filtered out from the stream") {
    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def read(id: ExpressionId): Option[ExpressionDetails] = {
        if (id.key == "c1") {
          throw new Exception("Read failed. Test error")
        }
        None
      }
    }

    val reports = List(
      Report(
        1551820461000L,
        ExpressionId("c1", ForwardingExpression("", "", None, "")),
        None,
        None
      ),
      Report(
        1551820461000L,
        ExpressionId("c2", ForwardingExpression("", "", None, "")),
        None,
        None
      )
    )

    val future = Source(reports)
      .via(readExprDetails(exprDetailsDao))
      .runWith(Sink.seq)
    val actual = Await.result(future, Duration.Inf)

    assert(actual === List(None))
  }

  test("Lookup scaling policy") {
    val ec2Policy1 = ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil)
    val data = Map(EddaEndpoint("123", "us-east-1", "local") -> List(ec2Policy1))
    system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDaoTestImpl(Map.empty[EddaEndpoint, List[ScalingPolicy]]),
          data
        )
      ),
      "scalingPolicies1"
    )
    val scalingPolicies = system.actorSelection("/user/scalingPolicies1")

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "123", Some("us-east-1"), "metric1")),
      Some(FwdMetricInfo("us-east-1", "123", "metric1", Map.empty[String, String])),
      None
    )

    val future = Source
      .single(report)
      .via(lookupScalingPolicy(scalingPolicies))
      .runWith(Sink.head)
    val actual = Await.result(future, Duration.Inf)
    val expected = ScalingPolicyStatus(
      false,
      Some(ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil))
    )
    assert(actual === expected)
  }

  test("Unknown scaling policy for no data") {
    val data = Map.empty[EddaEndpoint, List[ScalingPolicy]]
    system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDaoTestImpl(data),
          data
        )
      ),
      "scalingPolicies2"
    )
    val scalingPolicies = system.actorSelection("/user/scalingPolicies2")

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )

    val future = Source
      .single(report)
      .via(lookupScalingPolicy(scalingPolicies))
      .runWith(Sink.head)
    val actual = Await.result(future, Duration.Inf)
    val expected = ScalingPolicyStatus(true, None)
    assert(actual === expected)
  }

  test("Unknown scaling policy for dao errors") {
    val data = Map.empty[EddaEndpoint, List[ScalingPolicy]]
    system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          () => {
            Flow[EddaEndpoint]
              .filter(_ => false)
              .map(_ => List.empty[ScalingPolicy])
          },
          data
        )
      ),
      "scalingPolicies3"
    )
    val scalingPolicies = system.actorSelection("/user/scalingPolicies3")

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "123", Some("us-east-1"), "metric1")),
      Some(FwdMetricInfo("us-east-1", "123", "metric1", Map.empty[String, String])),
      None
    )

    val future = Source
      .single(report)
      .via(lookupScalingPolicy(scalingPolicies))
      .runWith(Sink.head)
    val actual = Await.result(future, Duration.Inf)
    val expected = ScalingPolicyStatus(true, None)
    assert(actual === expected)
  }

  test("Save ExpressionDetails using a dedicated dispatcher") {
    val saved = List.newBuilder[ExpressionDetails]

    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def save(exprDetails: ExpressionDetails): Unit = {
        saved += exprDetails
      }
    }

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val exprDetails = ExpressionDetails(
      report.id,
      report.timestamp,
      Nil,
      report.error,
      Map.empty[String, Long],
      Nil
    )

    val future = Source
      .single(exprDetails)
      .via(saveExprDetails(exprDetailsDao))
      .runWith(Sink.head)
    val result = Await.result(future, Duration.Inf)

    assert(result === NotUsed)
    assert(saved.result() === List(exprDetails))
  }

  test("Save errors should be filtered out") {
    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def save(exprDetails: ExpressionDetails): Unit = {
        throw new Exception("Save failed. Test error")
      }
    }

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val exprDetails = ExpressionDetails(
      report.id,
      report.timestamp,
      Nil,
      report.error,
      Map.empty[String, Long],
      Nil
    )

    val future = Source
      .single(exprDetails)
      .via(saveExprDetails(exprDetailsDao))
      .runWith(Sink.headOption)
    val result = Await.result(future, Duration.Inf)

    assert(result === None)
  }
}
