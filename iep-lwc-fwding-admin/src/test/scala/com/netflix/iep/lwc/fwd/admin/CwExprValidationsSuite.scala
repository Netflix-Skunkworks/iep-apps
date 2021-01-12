/*
 * Copyright 2014-2021 Netflix, Inc.
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

import akka.actor.ActorSystem
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.funsuite.AnyFunSuite

class CwExprValidationsSuite
    extends AnyFunSuite
    with TestAssertions
    with CwForwardingTestConfig
    with StrictLogging {

  private val config = ConfigFactory.load()
  private val system = ActorSystem()

  private val validations = new CwExprValidations(
    new ExprInterpreter(config),
    new Evaluator(config, new NoopRegistry(), system)
  )

  test("Run all checks for a valid expression") {
    val config = makeConfigString()()
    validations.validate("foo", Json.decode[JsonNode](config))
  }

  test("Skip given validations for a valid expression") {
    val config = makeConfigString()(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,foo_app,:eq,
                   |  name,nodejs.cpuUsage,:eq,:and,
                   |  :node-avg,
                   |  (,nf.account,nf.asg,nf.stack,),:by
                 """.stripMargin,
      dimensions = """
                     | [
                     |   {
                     |     "name": "AutoScalingGroupName",
                     |     "value": "$(nf.asg)"
                     |   },
                     |   {
                     |     "name": "Stack",
                     |     "value": "$(nf.stack)"
                     |   }
                     | ]""".stripMargin,
      checksToSkip = """["AsgGrouping", "DefaultGrouping"]"""
    )

    validations.validate("foo", Json.decode[JsonNode](config))
  }

  test("Should fail for an invalid expression") {
    val config = makeConfigString()(
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

    assertFailure(
      validations.validate("foo", Json.decode[JsonNode](config)),
      "More than one expression found"
    )
  }

  test("Valid `checksToSkip` list") {
    validations.validateChecksToSkip(
      makeConfig(checksToSkip = List("AsgGrouping"))
    )
  }

  test("`checksToSkip` cannot contain a key of a required check") {
    assertFailure(
      validations.validateChecksToSkip(
        makeConfig(checksToSkip = List("SingleExpression"))
      ),
      "SingleExpression cannot be optional"
    )
  }

  test("`checksToSkip` cannot contain a missing key") {
    assertFailure(
      validations.validateChecksToSkip(
        makeConfig(checksToSkip = List("InvalidCheckName"))
      ),
      "Invalid validation: InvalidCheckName"
    )
  }

}
