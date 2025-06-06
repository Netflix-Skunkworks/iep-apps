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
package com.netflix.atlas.druid

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class DruidFilterSuite extends FunSuite {

  private val interpreter = new Interpreter(QueryVocabulary.allWords)

  private def eval(s: String): Query = {
    interpreter.execute(s).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalArgumentException("invalid expression")
    }
  }

  test("forQuery - nf.datasource :eq") {
    val actual = DruidFilter.forQuery(eval("nf.datasource,foo,:eq"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - nf.datasource :re") {
    val actual = DruidFilter.forQuery(eval("nf.datasource,foo,:re"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - nf.datasource :has") {
    val actual = DruidFilter.forQuery(eval("nf.datasource,:has"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - nf.datasource :not") {
    val actual = DruidFilter.forQuery(eval("nf.datasource,foo,:eq,:not"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - name :eq") {
    val actual = DruidFilter.forQuery(eval("name,foo,:eq"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - name :not") {
    val actual = DruidFilter.forQuery(eval("name,foo,:eq,:not"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - percentile :eq") {
    val actual = DruidFilter.forQuery(eval("percentile,T0090,:eq"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - percentile :lt") {
    val actual = DruidFilter.forQuery(eval("percentile,T0090,:lt"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("forQuery - percentile :and") {
    val actual = DruidFilter.forQuery(eval("country,US,:eq,percentile,T0090,:lt,:and"))
    val expected = Some(DruidFilter.Equal("country", "US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :true") {
    val actual = DruidFilter.forQuery(eval(":true"))
    val expected = None
    assertEquals(actual, expected)
  }

  test("toFilter - :true") {
    intercept[UnsupportedOperationException] {
      DruidFilter.toFilter(eval(":true"))
    }
  }

  test("forQuery - :false") {
    intercept[UnsupportedOperationException] {
      DruidFilter.forQuery(eval(":false"))
    }
  }

  test("forQuery - :eq") {
    val actual = DruidFilter.forQuery(eval("country,US,:eq"))
    val expected = Some(DruidFilter.Equal("country", "US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :re") {
    val actual = DruidFilter.forQuery(eval("country,US,:re"))
    val expected = Some(DruidFilter.Regex("country", "^US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :reic") {
    DruidFilter.forQuery(eval("country,US,:reic"))
    val actual = DruidFilter.forQuery(eval("country,US,:reic"))
    val expected =
      Some(DruidFilter.Regex("country", "(?i)^US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :has") {
    val actual = DruidFilter.forQuery(eval("country,:has"))
    val expected = Some(DruidFilter.Not(DruidFilter.Equal("country", "")))
    assertEquals(actual, expected)
  }

  test("forQuery - :in") {
    val actual = DruidFilter.forQuery(eval("country,(,US,CA,),:in"))
    val expected = Some(DruidFilter.In("country", List("US", "CA")))
    assertEquals(actual, expected)
  }

  test("forQuery - :gt") {
    val actual = DruidFilter.forQuery(eval("country,US,:gt"))
    val expected = Some(DruidFilter.Bound.greaterThan("country", "US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :ge") {
    val actual = DruidFilter.forQuery(eval("country,US,:ge"))
    val expected = Some(DruidFilter.Bound.greaterThanEqual("country", "US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :lt") {
    val actual = DruidFilter.forQuery(eval("country,US,:lt"))
    val expected = Some(DruidFilter.Bound.lessThan("country", "US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :le") {
    val actual = DruidFilter.forQuery(eval("country,US,:le"))
    val expected = Some(DruidFilter.Bound.lessThanEqual("country", "US"))
    assertEquals(actual, expected)
  }

  test("forQuery - :le sanitize single quote") {
    val actual = DruidFilter.forQuery(eval("country,US',:le"))
    val expected = Some(DruidFilter.Bound.lessThanEqual("country", "US'"))
    assertEquals(actual, expected)
  }

  test("forQuery - :le sanitize double quote") {
    val actual = DruidFilter.forQuery(eval("country,US\",:le"))
    val expected = Some(DruidFilter.Bound.lessThanEqual("country", "US\""))
    assertEquals(actual, expected)
  }

  test("forQuery - :and") {
    val actual = DruidFilter.forQuery(eval("country,(,US,CA,),:in,device,xbox,:eq,:and"))
    val expected = Some(
      DruidFilter.And(
        List(
          DruidFilter.In("country", List("US", "CA")),
          DruidFilter.Equal("device", "xbox")
        )
      )
    )
    assertEquals(actual, expected)
  }

  test("forQuery - :or") {
    val actual = DruidFilter.forQuery(eval("country,(,US,CA,),:in,device,xbox,:eq,:or"))
    val expected = Some(
      DruidFilter.Or(
        List(
          DruidFilter.In("country", List("US", "CA")),
          DruidFilter.Equal("device", "xbox")
        )
      )
    )
    assertEquals(actual, expected)
  }

  test("forQuery - :not") {
    val actual = DruidFilter.forQuery(eval("country,(,US,CA,),:in,:not"))
    val expected = Some(DruidFilter.Not(DruidFilter.In("country", List("US", "CA"))))
    assertEquals(actual, expected)
  }
}
