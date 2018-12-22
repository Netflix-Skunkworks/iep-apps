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

import com.netflix.atlas.core.model.StyleExpr
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite

class CwExprValidationMethodsSuite
    extends FunSuite
    with TestAssertions
    with CwForwardingTestConfig
    with StrictLogging {

  val validations = new CwExprValidations(
    new ExprInterpreter(ConfigFactory.load())
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
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(validations.singleExpression(expr, styleExpr), "More than one expression found")
  }

  test("Dimensions cannot be empty by default") {
    val expr = Expression("", "", Seq.empty[Dimension], "")
    assertFailure(
      validations.asgGrouping(expr, List.empty[StyleExpr]),
      "Only `AutoScalingGroupName` dimension allowed by default and should use nf.asg grouping for value"
    )
  }

  test("Only one dimension allowed by default") {
    val expr = Expression("", "", Seq(Dimension("d1", "v1"), Dimension("d2", "v2")), "")
    assertFailure(
      validations.asgGrouping(expr, List.empty[StyleExpr]),
      "Only `AutoScalingGroupName` dimension allowed by default and should use nf.asg grouping for value"
    )
  }

  test("Only AutoScalingGroupName allowed by default") {
    val expr = Expression("", "", Seq(Dimension("d1", "v1")), "")
    assertFailure(
      validations.asgGrouping(expr, List.empty[StyleExpr]),
      "Only `AutoScalingGroupName` dimension allowed by default and should use nf.asg grouping for value"
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
      dimensions = Seq(Dimension("AutoScalingGroupName", "$(nf.asg)"))
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(
      validations.asgGrouping(expr, styleExpr),
      "Only `AutoScalingGroupName` dimension allowed by default and should use nf.asg grouping for value"
    )
  }

  test("Valid default dimension") {
    val config = makeConfig()

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    validations.asgGrouping(expr, styleExpr)
  }

  test("Account should be a variable by default") {
    val expr = Expression("", "", Seq.empty[Dimension], "3456")
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
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(
      validations.accountGrouping(expr, styleExpr),
      "Account by default should use nf.account grouping for value"
    )
  }

  test("Valid account") {
    val config = makeConfig()

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    validations.accountGrouping(expr, styleExpr)
  }

  test("All grouping tags should be mapped") {
    val config = makeConfig(
      dimensions = Seq.empty[Dimension],
      account = "$(nf.account)"
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(
      validations.allGroupingsMapped(expr, styleExpr),
      "Variable mapping missing for grouping tags [nf.asg]"
    )
  }

  test("Account variable is not mapped") {
    val config = makeConfig(account = "3456")

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(
      validations.allGroupingsMapped(expr, styleExpr),
      "Variable mapping missing for grouping tags [nf.account]"
    )
  }

  test("Valid grouping tags mapping") {
    val config = makeConfig(
      metricName = "$(customName)",
      atlasUri = """
              | http://localhost/api/v1/graph?q=
              |  nf.app,foo_app1,:eq,
              |  name,nodejs.cpuUsage,:eq,:and,
              |  (,customName,nf.account,nf.asg,),:by
            """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)
    validations.allGroupingsMapped(expr, styleExpr)
  }

  test("Valid grouping tags mapping with constant account") {
    val config = makeConfig(
      dimensions =
        Seq(Dimension("AutoScalingGroupName", "$(nf.asg)"), Dimension("Account", "$(nf.account)")),
      account = "3456"
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)
    validations.allGroupingsMapped(expr, styleExpr)
  }

  test("Variables used should be part of exact match or grouping keys") {
    val config = makeConfig(
      dimensions =
        Seq(Dimension("AutoScalingGroupName", "$(nf.asg)"), Dimension("Zone", "$(nf.zone)"))
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(
      validations.variablesSubstitution(expr, styleExpr),
      "Variables not found in exact match or in grouping keys [nf.zone]"
    )
  }

  test("Valid variable substitution") {
    val config = makeConfig(
      dimensions =
        Seq(Dimension("AutoScalingGroupName", "$(nf.asg)"), Dimension("App", "$(nf.app)"))
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    validations.variablesSubstitution(expr, styleExpr)
  }

  test("Exact match should be done for high cardinality groupings") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,nq_android_api,:eq,
                   |  name,cgroup,:re,:and,
                   |  (,name,nf.asg,nf.account,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    assertFailure(
      validations.unpredictableNoOfMetrics(expr, styleExpr),
      "No of forwarded metrics might be very high because of grouping [name]"
    )
  }

  test("Valid expression with exact match for high cardinality groupings") {
    val config = makeConfig(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,nq_android_api,:eq,
                   |  name,cgroup.mem.used,:eq,:and,
                   |  (,name,nf.asg,nf.account,),:by
                 """.stripMargin
    )

    val expr = config.expressions.head
    val styleExpr = validations.interpreter.eval(expr.atlasUri)

    validations.unpredictableNoOfMetrics(expr, styleExpr)
  }
}
