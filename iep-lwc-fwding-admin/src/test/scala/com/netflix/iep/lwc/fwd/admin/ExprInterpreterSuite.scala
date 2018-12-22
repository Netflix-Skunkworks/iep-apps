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

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class ExprInterpreterSuite extends FunSuite {
  val interpreter = new ExprInterpreter(ConfigFactory.load())

  test("Should be able to parse a valid expression") {
    val atlasUri = makeAtlasUri(
      expr = """
               |name,nodejs.cpuUsage,:eq,
               |:node-avg,
               |(,nf.account,nf.asg,),:by
             """.stripMargin
    )

    assert(interpreter.eval(atlasUri).size == 1)
  }

  test("Should fail for invalid expression") {
    val atlasUri = makeAtlasUri(
      expr = """
               |name,nodejs.cpuUsage,:e,
               |:node-avg,
               |(,nf.account,nf.asg,),:by
             """.stripMargin
    )

    val exception = intercept[IllegalStateException](
      interpreter.eval(atlasUri)
    )
    assert(exception.getMessage == "unknown word ':e'")
  }

  def makeAtlasUri(
    expr: String
  ): String = {
    s"""http://localhost/api/v1/graph?q=$expr"""
      .replace("\n", "")
      .trim
  }

}
