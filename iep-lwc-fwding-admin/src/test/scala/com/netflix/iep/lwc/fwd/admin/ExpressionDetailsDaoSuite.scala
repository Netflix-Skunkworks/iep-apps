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
          |  name,nodejs.cpuUsage,:eq,:and,
          |  :node-avg,
          |  (,nf.account,nf.asg,),:by
        """.stripMargin
          .filterNot(_.isWhitespace)
          .replace("\n", ""),
        "$(nf.account)",
        Some("us-east-1"),
        "nodejs.cpuUsage",
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
      Some(
        FwdMetricInfo(
          "us-east-1",
          "1234",
          "nodejs.cpuUsage",
          Map("AutoScalingGroup" -> "asg1-v00")
        )
      ),
      None,
      Map.empty[String, Long],
      None
    )

    dao.save(exprDetails)
    val actual = dao.read(id)
    assert(actual === Some(exprDetails))

  }

  localTest("Query purge eligible") {
    val id = ExpressionId("", ForwardingExpression("", "", None, "", Nil))

    val reportTs = 1551820461000L
    val timestampThen = 1551820461000L + 11.minutes.toMillis

    val exprDetailsList = List(
      new ExpressionDetails(
        id.copy(key = "config2"),
        reportTs,
        None,
        None,
        Map.empty[String, Long],
        None
      ),
      new ExpressionDetails(
        id.copy(key = "config3"),
        reportTs,
        None,
        None,
        Map(NoDataFoundEvent -> reportTs),
        None
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
