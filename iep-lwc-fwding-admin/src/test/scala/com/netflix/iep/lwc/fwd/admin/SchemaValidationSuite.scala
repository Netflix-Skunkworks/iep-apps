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
import com.netflix.atlas.json.Json
import org.scalatest.FunSuite

class SchemaValidationSuite extends FunSuite with TestAssertions with CwForwardingTestConfig {

  val schemaValidation = new SchemaValidation

  test("Valid configuration") {
    validate(makeConfigString()())
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
      validate(makeConfigString()(email = "app-oncall")),
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
        |      "atlasUri": "http://localhost/api/v1/graph?q=query"
        |    }
        |  ]
        |}
      """.stripMargin
    assertFailure(
      validate(config),
      "object has missing required properties ([\"account\"])"
    )
  }

  test("Fail for invalid metric name") {
    assertFailure(
      validate(makeConfigString()(metricName = "nodejs,cpuUsage")),
      "ECMA 262 regex \"^([\\w\\-\\.]+|\\$\\([\\w\\-\\.]+\\))$\" does not match " +
      "input string \"nodejs,cpuUsage\""
    )
  }

  test("Fail for invalid atlasUri name") {
    assertFailure(
      validate(makeConfigString()(atlasUri = "http://localhost?q=query")),
      "ECMA 262 regex \"^(https?://)?[\\w-]+(\\.[\\w-]+)*(:\\d+)?/api/v(\\d+){1}/graph\\?.+$\" does not match input string \"http://localhost?q=query\""
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

  test("Allow empty array for dimensions") {
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

    validate(config)
  }

  test("Fail for invalid dimension name") {
    assertFailure(
      validate(makeConfigString(dimensionName = "ASG,Name")()),
      "ECMA 262 regex \"^[\\w\\-\\.]+$\" does not match input string \"ASG,Name\""
    )
  }

  test("Allow hardcoded asg name") {
    validate(makeConfigString(dimensionValue = "foo-test-v001")())
  }

  test("Dimension value cannot be empty") {
    assertFailure(
      validate(makeConfigString(dimensionValue = "")()),
      "ECMA 262 regex \"^([\\w\\-\\.]+|\\$\\([\\w\\-\\.]+\\))$\" does not match input string \"\""
    )
  }

  test("Fail for invalid dimension value") {
    assertFailure(
      validate(makeConfigString(dimensionValue = "$(nf:asg)")()),
      "ECMA 262 regex \"^([\\w\\-\\.]+|\\$\\([\\w\\-\\.]+\\))$\" does not match input string \"$(nf:asg)\""
    )
  }

  test("Cannot mix hardcoded value and a variable for dimension") {
    assertFailure(
      validate(makeConfigString(dimensionValue = "foo-test-v001$(nf.asg)")()),
      "ECMA 262 regex \"^([\\w\\-\\.]+|\\$\\([\\w\\-\\.]+\\))$\" does not match input string \"foo-test-v001$(nf.asg)\""
    )
  }

  test("Allow hardcoded account id") {
    validate(makeConfigString()(account = "23456"))
  }

  test("Account value cannot by empty") {
    assertFailure(
      validate(makeConfigString()(account = "")),
      "ECMA 262 regex \"^([\\d]+|\\$\\([\\w\\-\\.]+\\))$\" does not match input string \"\""
    )
  }

  test("Fail for invalid account variable") {
    assertFailure(
      validate(makeConfigString()(account = "$(nf:account)")),
      "ECMA 262 regex \"^([\\d]+|\\$\\([\\w\\-\\.]+\\))$\" does not match input string \"$(nf:account)\""
    )
  }

  test("Cannot mix hardcoded value and a variable for account") {
    assertFailure(
      validate(makeConfigString()(account = "123$(nf.account)")),
      "ECMA 262 regex \"^([\\d]+|\\$\\([\\w\\-\\.]+\\))$\" does not match input string \"123$(nf.account)\""
    )
  }

  test("checksToSkip cannot be empty") {
    assertFailure(
      validate(makeConfigString()(checksToSkip = "[]")),
      "array is too short: must have at least 1 elements but instance has 0 elements"
    )
  }

  test("checksToSkip entries cannot be empty") {
    assertFailure(
      validate(makeConfigString()(checksToSkip = """[""]""")),
      "string \"\" is too short (length: 0, required minimum: 1)"
    )
  }

  test("checksToSkip should be a string array") {
    assertFailure(
      validate(makeConfigString()(checksToSkip = "[1]")),
      "instance type (integer) does not match any allowed primitive type (allowed: [\"string\"])"
    )
  }

  private def validate(input: String): Unit = {
    schemaValidation.validate("cfg1", Json.decode[JsonNode](input))
  }

}
