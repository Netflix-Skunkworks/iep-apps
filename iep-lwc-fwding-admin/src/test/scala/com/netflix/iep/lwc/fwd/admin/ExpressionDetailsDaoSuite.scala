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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.netflix.iep.aws.AwsClientFactory
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingDimension
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite

class ExpressionDetailsDaoSuite extends FunSuite with StrictLogging {

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
      System.currentTimeMillis(),
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

    val dao = new ExpressionDetailsDaoImpl(makeDynamoDBClient())
    dao.save(exprDetails)
    val actual = dao.read(id)
    assert(actual === Some(exprDetails))
  }

  def makeDynamoDBClient(): AmazonDynamoDB = {
    new AwsClientFactory(ConfigFactory.load())
      .newInstance(classOf[AmazonDynamoDB])
  }

  def localTest(testName: String)(testFun: => Any): Unit = {
    if (sys.env.get("LOCAL_TESTS").isDefined) {
      test(testName)(testFun)
    } else {
      logger.info(s"Skipped local only test: $testName")
    }
  }

}
