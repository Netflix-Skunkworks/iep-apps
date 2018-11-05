/*
 * Copyright 2014-2018 Netflix, Inc.
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

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.netflix.atlas.json.Json
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
import scala.io.Source

class JsonSchemaSuite extends FunSuite {

  val schema = JsonSchemaFactory
    .byDefault()
    .getJsonSchema(
      Json.decode[JsonNode](
        Source.fromResource("cluster-config-schema.json").reader()
      )
    )

  test("Valid configuration") {
    val (isSuccess, msgs) = validate(makeConfig())

    assert(isSuccess)
    assert(msgs.isEmpty)
  }

  test("Fail when top level node is not an object") {
    assertFailure(
      validate("[]"),
      "instance type (array) does not match any allowed primitive type (allowed: [\"object\"])"
    )
  }

  test("Fail for missing fields") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com"
        |}
      """.stripMargin

    assertFailure(
      validate(config),
      "object has missing required properties ([\"expressions\"])"
    )
  }

  test("Fail for invalid email") {
    assertFailure(
      validate(makeConfig(email = "app-oncall")),
      "string \"app-oncall\" is not a valid email address"
    )
  }

  test("Fail for invalid expression type") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": ""
        |}
      """.stripMargin

    assertFailure(
      validate(config),
      "instance type (string) does not match any allowed primitive type (allowed: [\"array\"])"
    )
  }

  test("Fail when no expression is found") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": []
        |}
      """.stripMargin

    assertFailure(
      validate(config),
      "array is too short: must have at least 1 elements but instance has 0 elements"
    )
  }

  test("Fail for missing fields in an expression") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": [
        |    {
        |      "metricName": "nodejs.cpuUsage",
        |      "atlasUri": "http://localhost/api/v1/graph?q=query",
        |      "account": "$(account)"
        |    }
        |  ]
        |}
      """.stripMargin
    assertFailure(
      validate(config),
      "object has missing required properties ([\"dimensions\"])"
    )
  }

  test("Fail for invalid metric name") {
    assertFailure(
      validate(makeConfig(metricName = ".nodejs.cpuUsage")),
      "ECMA 262 regex \"^[a-zA-Z0-9]+[a-zA-Z0-9_\\-\\.]*[a-zA-Z0-9]+$\" does not match input string \".nodejs.cpuUsage\""
    )
  }

  test("Fail for invalid atlasUri name") {
    assertFailure(
      validate(makeConfig(atlasUri = "http://localhost/api/v1/graph?q=^query")),
      "string \"http://localhost/api/v1/graph?q=^query\" is not a valid URI"
    )
  }

  test("Fail for invalid dimensions type") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": [
        |    {
        |      "metricName": "nodejs.cpuUsage",
        |      "atlasUri": "http://localhost/api/v1/graph?q=query",
        |      "dimensions": {},
        |      "account": "$(account)"
        |    }
        |  ]
        |}
      """.stripMargin

    assertFailure(
      validate(config),
      "instance type (object) does not match any allowed primitive type (allowed: [\"array\"])"
    )
  }

  test("Fail when no dimensions found") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": [
        |    {
        |      "metricName": "nodejs.cpuUsage",
        |      "atlasUri": "http://localhost/api/v1/graph?q=query",
        |      "dimensions": [],
        |      "account": "$(account)"
        |    }
        |  ]
        |}
      """.stripMargin
    assertFailure(
      validate(config),
      "array is too short: must have at least 1 elements but instance has 0 elements"
    )
  }

  test("Fail for invalid dimension name") {
    assertFailure(
      validate(makeConfig(dimensionName = "_ASGName")),
      "ECMA 262 regex \"^[a-zA-Z0-9]+[a-zA-Z0-9_\\-\\.]*[a-zA-Z0-9]+$\" does not match input string \"_ASGName\""
    )
  }

  test("Fail for invalid dimension value") {
    assertFailure(
      validate(makeConfig(dimensionValue = "asg")),
      "ECMA 262 regex \"^\\$\\([a-zA-Z0-9]+[a-zA-Z0-9\\.]*[a-zA-Z0-9]+\\)$\" does not match input string \"asg\""
    )
  }

  test("Fail for invalid account variable") {
    assertFailure(
      validate(makeConfig(account = "account")),
      "ECMA 262 regex \"^\\$\\([a-zA-Z0-9]+[a-zA-Z0-9\\.]*[a-zA-Z0-9]+\\)$\" does not match input string \"account\""
    )
  }

  private def makeConfig(
    email: String = "app-oncall@netflix.com",
    metricName: String = "nodejs.cpuUsage",
    atlasUri: String = "http://localhost/api/v1/graph?q=query",
    dimensionName: String = "AutoScalingGroupName",
    dimensionValue: String = "$(asg)",
    account: String = "$(account)"
  ): String = {
    s"""
        |{
        |  "email": "$email",
        |  "expressions": [
        |    {
        |      "metricName": "$metricName",
        |      "atlasUri": "$atlasUri",
        |      "dimensions": [
        |        {
        |          "name": "$dimensionName",
        |          "value": "$dimensionValue"
        |        }
        |      ],
        |      "account": "$account"
        |    }
        |  ]
        |}
      """.stripMargin
  }

  private def validate(input: String): (Boolean, Iterable[String]) = {
    val report = schema.validate(Json.decode[JsonNode](input))
    (report.isSuccess(), report.asScala.map(_.getMessage))
  }

  private def assertFailure(report: (Boolean, Iterable[String]), expectedMsg: String): Unit = {
    val (isSuccess, msgs) = report

    assert(!isSuccess)
    assert(msgs.exists(_ == expectedMsg))
  }

}
