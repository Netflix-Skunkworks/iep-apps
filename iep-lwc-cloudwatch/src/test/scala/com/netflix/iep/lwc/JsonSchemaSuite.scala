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

import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONArray, JSONObject, JSONTokener}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.{Failure, Try}

class JsonSchemaSuite extends FunSuite {

  val schema = SchemaLoader.load(
    new JSONObject(
      new JSONTokener(Source.fromResource("cluster-config-schema.json").reader())
    )
  )

  test("Valid configuration") {
    assert(Try(schema.validate(new JSONObject(makeConfig()))).isSuccess)
  }

  test("Fail when top level node is not an object") {
    assertException(schema.validate(new JSONArray()), "#: expected type: JSONObject, found: JSONArray")
  }

  test("Fail for missing fields") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com"
        |}
      """.stripMargin

    assertException(schema.validate(new JSONObject(config)), "#: required key [expressions] not found")
  }

  test("Fail for invalid email") {
    val config = makeConfig(email = "app-oncall")
    assertException(schema.validate(new JSONObject(config)), "#/email: [app-oncall] is not a valid email address")
  }

  test("Fail for invalid expression type") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": ""
        |}
      """.stripMargin

    assertException(schema.validate(new JSONObject(config)), "#/expressions: expected type: JSONArray, found: String")
  }

  test("Fail when no expression is found") {
    val config =
      """
        |{
        |  "email": "app-oncall@netflix.com",
        |  "expressions": []
        |}
      """.stripMargin

    assertException(schema.validate(new JSONObject(config)), "#/expressions: expected minimum item count: 1, found: 0")
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
    assertException(schema.validate(new JSONObject(config)), "#/expressions/0: required key [dimensions] not found")
  }

  test("Fail for invalid metric name") {
    val config = makeConfig(metricName = ".nodejs.cpuUsage")
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/metricName: string [.nodejs.cpuUsage] does not match pattern ^[a-zA-Z0-9]+[a-zA-Z0-9_\\-\\.]*[a-zA-Z0-9]+$"
    )
  }

  test("Fail for invalid atlasUri name") {
    val config = makeConfig(atlasUri = "http://localhost/api/v1/graph?q=^query")
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/atlasUri: [http://localhost/api/v1/graph?q=^query] is not a valid URI"
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
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/dimensions: expected type: JSONArray, found: JSONObject"
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
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/dimensions: expected minimum item count: 1, found: 0"
    )
  }

  test("Fail for invalid dimension name") {
    val config = makeConfig(dimensionName = "_ASGName")
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/dimensions/0/name: string [_ASGName] does not match pattern ^[a-zA-Z0-9]+[a-zA-Z0-9_\\-\\.]*[a-zA-Z0-9]+$"
    )
  }

  test("Fail for invalid dimension value") {
    val config = makeConfig(dimensionValue = "asg")
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/dimensions/0/value: string [asg] does not match pattern ^\\$\\([a-zA-Z0-9]+[a-zA-Z0-9\\.]*[a-zA-Z0-9]+\\)$"
    )
  }

  test("Fail for invalid account variable") {
    val config = makeConfig(account = "account")
    assertException(
      schema.validate(new JSONObject(config)),
      "#/expressions/0/account: string [account] does not match pattern ^\\$\\([a-zA-Z0-9]+[a-zA-Z0-9\\.]*[a-zA-Z0-9]+\\)$"
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

  private def assertException(f: => Any, message: String): Unit = {
    Try(f) match {
      case Failure(e) => assert(message == e.getMessage())
      case _ => fail()
    }
  }
}
