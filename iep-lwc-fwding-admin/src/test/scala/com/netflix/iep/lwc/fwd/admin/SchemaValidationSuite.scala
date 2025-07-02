/*
 * Copyright 2014-2025 Netflix, Inc.
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
import munit.FunSuite

class SchemaValidationSuite extends FunSuite with TestAssertions with CwForwardingTestConfig {

  val schemaValidation = new SchemaValidation

  test("Valid configuration") {
    validate(makeConfigString())
  }

  test("Fail when top level node is not an object") {
    assertFailure(
      validate("[]"),
      "does not match any allowed primitive type"
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
      "object has missing required properties"
    )
  }

  test("Fail for invalid email") {
    assertFailure(
      validate(makeConfigString(email = "app-oncall")),
      "not a valid email address"
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
      "does not match any allowed primitive type"
    )
  }

  test("Allow empty list for expressions") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": []
        |}
      """.stripMargin
    validate(config)
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
      "object has missing required properties"
    )
  }

  test("Valid metric names") {
    Seq(
      "cons",
      "cons cons",
      "$(var)",
      "cons$(var)",
      "$(var)cons",
      "$(var)$(var)",
      "cons$(var)cons",
      "$(var)cons$(var)"
    ).foreach(n => validate(makeConfigString(metricName = n)))
  }

  test("Fail for invalid metric name") {
    Seq(
      "",
      "nodejs,cpuUsage",
      "${var}",
      "$(nf:asg)",
      "$(nf: asg)"
    ).foreach { n =>
      assertFailure(
        validate(makeConfigString(metricName = n)),
        "does not match input string"
      )
    }
  }

  test("Fail for invalid atlasUri name") {
    assertFailure(
      validate(makeConfigString(atlasUri = "http://localhost?q=query")),
      "does not match input string"
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
      "does not match any allowed primitive type"
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
    Seq(
      "",
      "asg:name",
      "asg name"
    ).foreach { d =>
      assertFailure(
        validate(makeConfigString().replace("$(nf.asg)", d)),
        "does not match input string"
      )
    }
  }

  test("Valid dimension values") {
    Seq(
      "cons",
      "$(var)",
      "cons$(var)",
      "$(var)cons",
      "$(var)$(var)",
      "cons$(var)cons",
      "$(var)cons$(var)"
    ).foreach(d => validate(makeConfigString().replace("$(nf.asg)", d)))
  }

  test("Fail for invalid dimension values") {
    Seq(
      "",
      "asg:value",
      "${var}",
      "$(nf:asg)"
    ).foreach { d =>
      assertFailure(
        validate(makeConfigString().replace("$(nf.asg)", d)),
        "does not match input string"
      )
    }
  }

  test("Allow hardcoded account id") {
    validate(makeConfigString(account = "23456"))
  }

  test("Fail for invalid accounts") {
    Seq(
      "",
      "$(nf:account)",
      "123$(nf.account)"
    ).foreach { a =>
      assertFailure(
        validate(makeConfigString(account = a)),
        "does not match input string"
      )
    }
  }

  test("Allow hardcoded region") {
    validate(makeConfigString(region = "us-east-1"))
  }

  test("Fail for invalid regions") {
    Seq(
      "",
      "$(nf:region)",
      "us-$(region)"
    ).foreach { r =>
      assertFailure(
        validate(makeConfigString(region = r)),
        "does not match input string"
      )
    }
  }

  test("checksToSkip can be empty") {
    validate(makeConfigString(checksToSkip = "[]"))
  }

  test("checksToSkip entries cannot be empty") {
    assertFailure(
      validate(makeConfigString(checksToSkip = """[""]""")),
      "string \"\" is too short"
    )
  }

  test("checksToSkip should be a string array") {
    assertFailure(
      validate(makeConfigString(checksToSkip = "[1]")),
      "does not match any allowed primitive type"
    )
  }

  private def validate(input: String): Unit = {
    schemaValidation.validate(Json.decode[JsonNode](input))
  }

}
