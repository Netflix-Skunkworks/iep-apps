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
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.netflix.iep.lwc.fwd.admin.ExpressionDetails._
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingDimension
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import scala.concurrent.duration._

class ExpressionDetailsDaoSuite extends FunSuite with BeforeAndAfter with StrictLogging {

  val dao = new ExpressionDetailsDaoImpl(ConfigFactory.load(), makeDynamoDBClient())

  def makeDynamoDBClient(): AmazonDynamoDB = {
    AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          "http://localhost:8000",
          "us-east-1"
        )
      )
      .build()
  }

  def localTest(testName: String)(testFun: => Any): Unit = {
    if (sys.env.get("LOCAL_TESTS").isDefined) {
      test(testName)(testFun)
    } else {
      logger.info(s"Skipped local only test: $testName")
    }
  }

  after {
    dao.scan().foreach(dao.delete(_))
  }

  localTest("Save ExpressionDetails") {
    val id = ExpressionId(
      "config1",
      ForwardingExpression(
        """
          | http://localhost/api/v1/graph?q=
          |  nf.app,foo_app1,:eq,
          |  name,metric1,:eq,:and,
          |  :node-avg,
          |  (,nf.account,nf.asg,),:by
        """.stripMargin
          .filterNot(_.isWhitespace)
          .replace("\n", ""),
        "$(nf.account)",
        Some("us-east-1"),
        "metric1",
        List(
          ForwardingDimension(
            "AutoScalingGroup",
            "$(nf.asg)"
          )
        )
      )
    )

    val exprDetails = new ExpressionDetails(
      id,
      1551820461000L,
      List(
        FwdMetricInfo(
          "us-east-1",
          "1234",
          "metric1",
          Map("AutoScalingGroup" -> "asg1-v00")
        )
      ),
      None,
      Map.empty[String, Long],
      List(ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil))
    )

    dao.save(exprDetails)
    val actual = dao.read(id)
    assert(actual === Some(exprDetails))

  }

  localTest("Query purge eligible") {
    val id = ExpressionId("", ForwardingExpression("", "", None, "", Nil))

    val reportTs = 1551820461000L
    val timestampThen = reportTs + 11.minutes.toMillis

    val exprDetailsList = List(
      new ExpressionDetails(
        id.copy(key = "config2"),
        reportTs,
        Nil,
        None,
        Map.empty[String, Long],
        Nil
      ),
      new ExpressionDetails(
        id.copy(key = "config3"),
        reportTs,
        Nil,
        None,
        Map(NoDataFoundEvent -> reportTs),
        Nil
      )
    )

    exprDetailsList.foreach(dao.save(_))
    val actual = dao.queryPurgeEligible(timestampThen, List(NoDataFoundEvent))

    assert(actual === List(id.copy(key = "config3")))
  }

  localTest("Fail querying purge eligible for unknown event markers") {
    assertThrows[IllegalArgumentException](dao.queryPurgeEligible(0L, List("foo")))
  }

  localTest("Fail querying purge eligible for no event markers") {
    assertThrows[IllegalArgumentException](dao.queryPurgeEligible(0L, Nil))
  }
}

class ExpressionDetailsSuite extends FunSuite with StrictLogging {
  test("Purge eligible expression") {
    val reportTs = 1551820461000L
    val timestampThen = reportTs + 11.minutes.toMillis

    val ed = new ExpressionDetails(
      ExpressionId("config1", ForwardingExpression("", "", None, "", Nil)),
      reportTs,
      Nil,
      None,
      Map(
        NoDataFoundEvent          -> reportTs,
        NoScalingPolicyFoundEvent -> timestampThen
      ),
      Nil
    )

    val actual = ed.isPurgeEligible(timestampThen, 10.minutes.toMillis)

    assert(actual)
  }

  test("Not eligible for purge for unknown events") {
    val reportTs = 1551820461000L
    val timestampThen = reportTs + 11.minutes.toMillis

    val ed = new ExpressionDetails(
      ExpressionId("config1", ForwardingExpression("", "", None, "", Nil)),
      reportTs,
      Nil,
      None,
      Map("foo" -> reportTs),
      Nil
    )

    val actual = ed.isPurgeEligible(timestampThen, 10.minutes.toMillis)

    assert(actual == false)
  }

  test("Not eligible for purge") {
    val reportTs = 1551820461000L
    val timestampThen = reportTs + 11.minutes.toMillis

    val ed = new ExpressionDetails(
      ExpressionId("config1", ForwardingExpression("", "", None, "", Nil)),
      reportTs,
      Nil,
      None,
      Map.empty[String, Long],
      Nil
    )

    val actual = ed.isPurgeEligible(timestampThen, 10.minutes.toMillis)

    assert(actual == false)
  }
}
