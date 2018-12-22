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

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeriesExpr
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

class CwExprValidations @Inject()(var interpreter: ExprInterpreter) extends StrictLogging {

  val validations = Seq(
    Validation("SingleExpression", true, singleExpression),
    Validation("AsgGrouping", false, asgGrouping),
    Validation("AccountGrouping", false, accountGrouping),
    Validation("AllGroupingsMapped", true, allGroupingsMapped),
    Validation("VariablesSubstitution", true, variablesSubstitution),
    Validation("UnpredictableNoOfMetrics", false, unpredictableNoOfMetrics)
  )

  val asgDimension = Dimension("AutoScalingGroupName", "$(nf.asg)")

  val varPattern = "^\\$\\(([\\w\\-\\.]+)\\)$".r

  val knownLowCardinalityKeys = Seq(
    "nf.account",
    "nf.asg",
    "nf.cluster",
    "nf.stack",
    "nf.region",
    "nf.zone"
  )

  def validate(key: String, json: JsonNode): Unit = {
    val config = Json.decode[CwForwardingConfig](json)

    validateChecksToSkip(config)

    config.expressions.foreach { expr =>
      val styleExprs = interpreter.eval(expr.atlasUri)
      validations.foreach(_.validate(config, expr, styleExprs))
    }

  }

  def validateChecksToSkip(config: CwForwardingConfig): Unit = {
    config.checksToSkip.foreach { name =>
      if (isRequiredValidation(name) != false) {
        throw new IllegalArgumentException(s"$name cannot be optional")
      }
    }
  }

  def isRequiredValidation(name: String): Boolean = {
    validations
      .find(_.name == name)
      .getOrElse(throw new IllegalArgumentException(s"Invalid validation: $name"))
      .required
  }

  def singleExpression(expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    if (styleExpr.size > 1) {
      throw new IllegalArgumentException("More than one expression found")
    }
  }

  def asgGrouping(expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    // By default allow only `AutoScalingGroupName` dimension and the asg value should
    // come from grouping the expression using `nf.asg`
    val valid =
    expr.dimensions.size == 1 &&
    expr.dimensions.head == asgDimension &&
    getOneTimeSeriesExpr(styleExpr).finalGrouping.contains("nf.asg")

    if (!valid) {
      throw new IllegalArgumentException(
        "Only `AutoScalingGroupName` dimension allowed by default and should use nf.asg grouping for value"
      )
    }

  }

  def accountGrouping(expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    // By default account should be a variable and the query should use nf.account grouping
    val valid =
    expr.account == "$(nf.account)" &&
    getOneTimeSeriesExpr(styleExpr).finalGrouping.contains("nf.account")

    if (!valid) {
      throw new IllegalArgumentException(
        "Account by default should use nf.account grouping for value"
      )
    }
  }

  def allGroupingsMapped(expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    // All the grouping tags should be mapped as a dimension. Tag nf.account is treated special. It should
    // be used in the Expression.account field or as a dimension.
    val missing =
      getOneTimeSeriesExpr(styleExpr).finalGrouping
        .foldLeft(Seq.empty[String]) { (missingGroupings, grouping) =>
          val varName = s"$$($grouping)"

          val valid =
          expr.dimensions.exists(_.value == varName) ||
          expr.account == varName ||
          expr.metricName == varName

          if (!valid) {
            missingGroupings :+ grouping
          } else {
            missingGroupings
          }
        }

    if (!missing.isEmpty) {
      throw new IllegalArgumentException(
        s"Variable mapping missing for grouping tags ${missing.mkString("[", ",", "]")}"
      )
    }
  }

  def variablesSubstitution(expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    // All variables used should be available in grouping or exact match keys
    val timeSeriesExpr = getOneTimeSeriesExpr(styleExpr)
    val dataExpr = getFirstDataExpr(timeSeriesExpr)

    val allKeys = timeSeriesExpr.finalGrouping ++ Query.exactKeys(dataExpr.query)

    val missing =
      (expr.dimensions.map(_.value) :+ expr.account :+ expr.metricName)
        .flatMap {
          _ match {
            case varPattern(variable) => Some(variable)
            case _                    => None
          }
        }
        .filterNot(allKeys.contains(_))

    if (!missing.isEmpty) {
      throw new IllegalArgumentException(
        s"Variables not found in exact match or in grouping keys ${missing.mkString("[", ",", "]")}"
      )
    }
  }

  def unpredictableNoOfMetrics(expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    // Any grouping key other than the whitelisted ones should be an exact match
    // to avoid unpredictable number of metrics
    val timeSeriesExpr = getOneTimeSeriesExpr(styleExpr)
    val exactKeys = Query.exactKeys(getFirstDataExpr(timeSeriesExpr).query)

    val highCardinalityKeys = timeSeriesExpr.finalGrouping
      .foldLeft(Seq.empty[String]) { (keys, grouping) =>
        val valid =
        knownLowCardinalityKeys.contains(grouping) ||
        exactKeys.contains(grouping)

        if (!valid) {
          keys :+ grouping
        } else {
          keys
        }
      }

    if (!highCardinalityKeys.isEmpty) {
      throw new IllegalArgumentException(
        s"No of forwarded metrics might be very high because of grouping ${highCardinalityKeys
          .mkString("[", ",", "]")}"
      )
    }
  }

  private def getOneTimeSeriesExpr(styleExpr: List[StyleExpr]): TimeSeriesExpr = {
    styleExpr.headOption
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
  fn: (Expression, List[StyleExpr]) => Unit
) {

  def validate(config: CwForwardingConfig, expr: Expression, styleExpr: List[StyleExpr]): Unit = {
    if (required || !config.shouldSkip(name)) {
      fn(expr, styleExpr)
    }
  }
}
