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

import java.time.Duration

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query._
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeriesExpr
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.ClusterConfig
import com.netflix.iep.lwc.fwd.cw.ForwardingDimension
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

class CwExprValidations @Inject()(
  interpreter: ExprInterpreter,
  evaluator: Evaluator
) extends StrictLogging {

  val validations = List(
    Validation("SingleExpression", true, singleExpression),
    Validation("ValidStreamingExpr", true, validStreamingExpr),
    Validation("AsgGrouping", false, asgGrouping),
    Validation("AccountGrouping", false, accountGrouping),
    Validation("AllGroupingsMapped", true, allGroupingsMapped),
    Validation("VariablesSubstitution", true, variablesSubstitution),
    Validation("DefaultGrouping", false, defaultGrouping)
  )

  val defaultDimensions = List(ForwardingDimension("AutoScalingGroupName", "$(nf.asg)"))

  val varPattern = "\\$\\(([\\w\\-\\.]+)\\)".r

  val defaultGroupingKeys = List(
    "nf.account",
    "nf.asg"
  )

  def validate(key: String, json: JsonNode): Unit = {
    val config = Json.decode[ClusterConfig](json)

    validateChecksToSkip(config)

    config.expressions.foreach { expr =>
      val styleExprs = eval(expr.atlasUri)
      validations.foreach(_.validate(config, expr, styleExprs))
    }

  }

  def eval(atlasUri: String): List[StyleExpr] = {
    interpreter.eval(atlasUri)
  }

  def validateChecksToSkip(config: ClusterConfig): Unit = {
    config.checksToSkip.foreach { name =>
      if (isRequiredValidation(name) != false) {
        throw new IllegalArgumentException(s"$name cannot be optional")
      }
    }
  }

  def isRequiredValidation(name: String): Boolean = {
    validations
      .find(_.name == name)
      .getOrElse(
        throw new IllegalArgumentException(s"Invalid validation: $name")
      )
      .required
  }

  def validStreamingExpr(expr: ForwardingExpression, styleExprs: List[StyleExpr]): Unit = {
    evaluator.validate(
      new Evaluator.DataSource("_", Duration.ZERO, expr.atlasUri)
    )
  }

  def singleExpression(expr: ForwardingExpression, styleExprs: List[StyleExpr]): Unit = {
    if (styleExprs.isEmpty) {
      throw new RuntimeException(s"Missing styleExpr")
    }

    if (styleExprs.size > 1) {
      throw new IllegalArgumentException("More than one expression found")
    }
  }

  def asgGrouping(expr: ForwardingExpression, styleExprs: List[StyleExpr]): Unit = {
    // By default allow only `AutoScalingGroupName` dimension and the asg value
    // should come from grouping the expression using `nf.asg`
    val valid = {
      expr.dimensions == defaultDimensions &&
      getOneTimeSeriesExpr(styleExprs).finalGrouping.contains("nf.asg")
    }

    if (!valid) {
      throw new IllegalArgumentException(
        "Only `AutoScalingGroupName` dimension allowed by default and should " +
        "use nf.asg grouping for value"
      )
    }

  }

  def accountGrouping(expr: ForwardingExpression, styleExprs: List[StyleExpr]): Unit = {
    // By default account should be a variable and the query should use
    // nf.account grouping
    val valid = {
      expr.account == "$(nf.account)" &&
      getOneTimeSeriesExpr(styleExprs).finalGrouping.contains("nf.account")
    }

    if (!valid) {
      throw new IllegalArgumentException(
        "Account by default should use nf.account grouping for value"
      )
    }
  }

  def allGroupingsMapped(
    expr: ForwardingExpression,
    styleExprs: List[StyleExpr]
  ): Unit = {
    // All the grouping keys should be used as a substitution variable in
    // a dimension, account, metric name or region.
    val missing =
      getOneTimeSeriesExpr(styleExprs).finalGrouping
        .foldLeft(List.empty[String]) { (missingGroupings, grouping) =>
          val varName = s"$$($grouping)"

          val valid = {
            expr.dimensions.exists(_.value.contains(varName)) ||
            expr.account == varName ||
            expr.metricName.contains(varName) ||
            expr.region.map(_ == varName).getOrElse(false)
          }

          if (!valid) {
            missingGroupings :+ grouping
          } else {
            missingGroupings
          }
        }

    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        "Variable mapping missing for grouping " +
        missing.mkString("[", ",", "]")
      )
    }
  }

  def variablesSubstitution(
    expr: ForwardingExpression,
    styleExprs: List[StyleExpr]
  ): Unit = {
    // All variables used should be available in grouping or exact match keys
    // Skip checking region when the variable is $(nf.region). This is
    // because, queries on regional clusters would contain `nf.region` in
    // the data but will not be found in the query.
    val timeSeriesExpr = getOneTimeSeriesExpr(styleExprs)
    val dataExpr = getFirstDataExpr(timeSeriesExpr)

    val allKeys = timeSeriesExpr.finalGrouping ++ exactKeys(dataExpr.query)

    val missing = (
      expr.dimensions.map(_.value) :+
      expr.account :+
      expr.metricName :+
      expr.region.getOrElse("")
    ).flatMap { value =>
        for (m <- varPattern.findAllMatchIn(value)) yield m.group(1)
      }
      .filter(_.nonEmpty)
      .filterNot(_ == "nf.region")
      .filterNot(allKeys.contains(_))

    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        "Variables not found in exact match or in grouping keys " +
        missing.mkString("[", ",", "]")
      )
    }
  }

  def defaultGrouping(
    expr: ForwardingExpression,
    styleExprs: List[StyleExpr]
  ): Unit = {
    // To avoid unpredictable number of metrics getting
    // forwarded to CW, by default allow only grouping by
    // `nf.account` and `nf.asg`

    val timeSeriesExpr = getOneTimeSeriesExpr(styleExprs)

    val keys = timeSeriesExpr.finalGrouping.diff(defaultGroupingKeys)

    if (keys.nonEmpty) {
      throw new IllegalArgumentException(
        s"By default allowing only grouping by $defaultGroupingKeys"
      )
    }
  }

  private def getOneTimeSeriesExpr(
    styleExprs: List[StyleExpr]
  ): TimeSeriesExpr = {
    styleExprs.headOption
      .getOrElse(throw new RuntimeException(s"Missing styleExpr"))
      .expr
  }

  private def getFirstDataExpr(timeSeriesExpr: TimeSeriesExpr): DataExpr = {
    timeSeriesExpr.dataExprs.headOption
      .getOrElse(throw new RuntimeException(s"Missing dataExpr"))
  }

}

case class Validation(
  name: String,
  required: Boolean,
  fn: (ForwardingExpression, List[StyleExpr]) => Unit
) {

  def validate(
    config: ClusterConfig,
    expr: ForwardingExpression,
    styleExprs: List[StyleExpr]
  ): Unit = {
    if (required || !config.shouldSkip(name)) {
      fn(expr, styleExprs)
    }
  }
}
