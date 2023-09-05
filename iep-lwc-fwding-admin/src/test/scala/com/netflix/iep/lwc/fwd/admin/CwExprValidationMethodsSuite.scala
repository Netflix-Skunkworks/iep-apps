/*
 * Copyright 2014-2023 Netflix, Inc.
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

import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.iep.lwc.fwd.cw.ForwardingDimension
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import munit.FunSuite

class CwExprValidationMethodsSuite
    extends FunSuite
    with TestAssertions
    with CwForwardingTestConfig
    with StrictLogging {

  private val config = ConfigFactory.load()
  private val system = ActorSystem()

  private val validations = new CwExprValidations(
    new ExprInterpreter(config),
    new Evaluator(config, new NoopRegistry(), system)
  )

  test("Only one expression allowed") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app1,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  :node-avg,
                   |  (,nf.account,nf.asg,),:by,
                   |
                   |  nf.app,foo_app2,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  :node-avg,
                   |  (,nf.account,nf.asg,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.singleExpression(expr, styleExpr),
      "More than one expression found"
    )
  }

  test("Reject expensive queries that are not allowed for streaming") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,nq_tvui_.*,:re,
                   |  name,nodejs.cpu.*,:re,:and,
                   |  :node-avg,
                   |  (,nf.account,nf.asg,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.validStreamingExpr(expr, styleExpr),
      "rejected expensive query"
    )
  }

  test("Unknown streaming backend") {
    val config = makeConfig(
      atlasUri = """
                   | http://unknown/api/v1/graph?q=
                   |  nf.app,nq_tvui_darwin,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  :node-avg,
                   |  (,nf.account,nf.asg,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    intercept[NoSuchElementException](validations.validStreamingExpr(expr, styleExpr))
  }

  test("Dimensions cannot be empty by default") {
    val expr = ForwardingExpression("", "", None, "", List.empty[ForwardingDimension])
    assertFailure(
      validations.asgGrouping(expr, List.empty[StyleExpr]),
      "Only `AutoScalingGroupName` dimension allowed by " +
        "default and should use nf.asg grouping for value"
    )
  }

  test("Only one dimension allowed by default") {
    val expr = ForwardingExpression(
      "",
      "",
      None,
      "",
      List(ForwardingDimension("d1", "v1"), ForwardingDimension("d2", "v2"))
    )
    assertFailure(
      validations.asgGrouping(expr, List.empty[StyleExpr]),
      "Only `AutoScalingGroupName` dimension allowed by " +
        "default and should use nf.asg grouping for value"
    )
  }

  test("Only AutoScalingGroupName allowed by default") {
    val expr = ForwardingExpression(
      "",
      "",
      None,
      "",
      List(ForwardingDimension("d1", "v1"))
    )
    assertFailure(
      validations.asgGrouping(expr, List.empty[StyleExpr]),
      "Only `AutoScalingGroupName` dimension allowed by " +
        "default and should use nf.asg grouping for value"
    )
  }

  test("Query should use nf.asg grouping") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app1,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  :node-avg,
                   |  (,nf.account,),:by
                 """.stripMargin,
      dimensions = List(ForwardingDimension("AutoScalingGroupName", "$(nf.asg)"))
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.asgGrouping(expr, styleExpr),
      "Only `AutoScalingGroupName` dimension allowed by " +
        "default and should use nf.asg grouping for value"
    )
  }

  test("Valid default dimension") {
    val config = makeConfig()

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    validations.asgGrouping(expr, styleExpr)
  }

  test("Account should be a variable by default") {
    val expr = ForwardingExpression("", "3456", None, "", List.empty[ForwardingDimension])
    assertFailure(
      validations.accountGrouping(expr, List.empty[StyleExpr]),
      "Account by default should use nf.account grouping for value"
    )
  }

  test("Query should use nf.account grouping") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app1,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  :node-avg,
                   |  (,nf.asg,),:by
                 """.stripMargin,
      account = "$(nf.account)"
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.accountGrouping(expr, styleExpr),
      "Account by default should use nf.account grouping for value"
    )
  }

  test("Valid account") {
    val config = makeConfig()

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    validations.accountGrouping(expr, styleExpr)
  }

  test("Asg grouping should be mapped") {
    val config = makeConfig(
      dimensions = List.empty[ForwardingDimension],
      account = "$(nf.account)"
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.allGroupingsMapped(expr, styleExpr),
      "Variable mapping missing for grouping [nf.asg]"
    )
  }

  test("Account grouping should be mapped") {
    val config = makeConfig(
      account = "3456"
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.allGroupingsMapped(expr, styleExpr),
      "Variable mapping missing for grouping [nf.account]"
    )
  }

  test("Grouping key for metric name should be mapped") {
    val config = makeConfig(
      metricName = "foo-metric",
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app1,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  (,nf.account,nf.asg,tag1,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.allGroupingsMapped(expr, styleExpr),
      "Variable mapping missing for grouping [tag1]"
    )
  }

  test("Region grouping should be mapped") {
    val atlasUri = """
                 | http://localhost/api/v1/graph?q=
                 |  nf.app,foo_app1,:eq,
                 |  name,nodejs.cpuUsage,:eq,:and,
                 |  (,nf.region,nf.account,nf.asg,),:by
               """.stripMargin
    Seq(
      makeConfig(
        atlasUri = atlasUri,
        region = None
      ),
      makeConfig(
        atlasUri = atlasUri,
        region = Some("us-east-1")
      )
    ).foreach { config =>
      val expr = config.expressions.head
      val styleExpr = validations.eval(expr.atlasUri)

      assertFailure(
        validations.allGroupingsMapped(expr, styleExpr),
        "Variable mapping missing for grouping [nf.region]"
      )
    }

  }

  test("Valid grouping keys mapping") {
    val config = makeConfig(
      metricName = "metric-$(tag1)",
      atlasUri = """
              | http://localhost/api/v1/graph?q=
              |  nf.app,foo_app1,:eq,
              |  name,nodejs.cpuUsage,:eq,:and,
              |  (,nf.region,nf.account,nf.asg,tag1,tag2,tag3,),:by
            """.stripMargin,
      dimensions = List(
        ForwardingDimension("AutoScalingGroupName", "$(nf.asg)"),
        ForwardingDimension("AddInfo", "$(tag2)-$(tag3)")
      )
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)
    validations.allGroupingsMapped(expr, styleExpr)
  }

  test("Variables should be part of exact match or grouping keys") {
    val config = makeConfig(
      dimensions = List(
        ForwardingDimension("AutoScalingGroupName", "$(nf.asg)"),
        ForwardingDimension("Zone", "$(nf.zone)")
      )
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.variablesSubstitution(expr, styleExpr),
      "Variables not found in exact match or in grouping " +
        "keys [nf.zone]"
    )
  }

  test("Valid variable substitution") {
    val config = makeConfig(
      dimensions = List(
        ForwardingDimension("AutoScalingGroupName", "$(nf.asg)"),
        ForwardingDimension("AddInfo", "$(nf.account)-$(nf.app)-$(nf.region)")
      )
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    validations.variablesSubstitution(expr, styleExpr)
  }

  test("By default allow only grouping by account and asg") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app1,:eq,
                   |  name,requestsCompleted,:eq,:and,
                   |  (,nf.account,nf.asg,statusCode,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)

    assertFailure(
      validations.defaultGrouping(expr, styleExpr),
      s"By default allowing only grouping by " +
        s"${validations.defaultGroupingKeys}"
    )
  }

  test("Valid expr using default grouping keys") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app1,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  (,nf.asg,nf.account,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.eval(expr.atlasUri)
    validations.defaultGrouping(expr, styleExpr)
  }

}
