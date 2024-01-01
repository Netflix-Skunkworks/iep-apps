/*
 * Copyright 2014-2024 Netflix, Inc.
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
import com.netflix.iep.lwc.fwd.cw.ForwardingDimension
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import munit.FunSuite

class ValidationSuite extends FunSuite with TestAssertions with CwForwardingTestConfig {

  test("Perform a required validation") {

    val validation = Validation(
      "RequiredCheck",
      true,
      (_, _) => throw new IllegalArgumentException("Validation failed")
    )

    assertFailure(
      validation.validate(
        makeConfig(),
        ForwardingExpression("", "", None, "", List.empty[ForwardingDimension]),
        List.empty[StyleExpr]
      ),
      "Validation failed"
    )

  }

  test("Perform an optional validation") {
    val validation = Validation(
      "OptionalCheck",
      false,
      (_, _) => throw new IllegalArgumentException("Validation failed")
    )

    assertFailure(
      validation.validate(
        makeConfig(),
        ForwardingExpression("", "", None, "", List.empty[ForwardingDimension]),
        List.empty[StyleExpr]
      ),
      "Validation failed"
    )

  }

  test("Skip an optional validation") {
    val validation = Validation(
      "OptionalCheck",
      false,
      (_, _) => throw new IllegalArgumentException("Validation failed")
    )

    validation.validate(
      makeConfig(checksToSkip = List("OptionalCheck")),
      ForwardingExpression("", "", None, "", List.empty[ForwardingDimension]),
      List.empty[StyleExpr]
    )
  }

}
