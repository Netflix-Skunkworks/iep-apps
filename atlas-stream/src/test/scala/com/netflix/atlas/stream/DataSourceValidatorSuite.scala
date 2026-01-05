/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.stream

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import munit.FunSuite

class DataSourceValidatorSuite extends FunSuite {

  private val validateNoop: DataSource => Unit = _ => ()
  private val validateException: DataSource => Unit = _ => { throw new Exception("validate error") }

  test("DataSourceInput - string - valid") {
    val stringValid =
      """[{"id":"abc", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(stringValid) match {
      case Right(dss) => assertEquals(dss.sources().size(), 1)
      case Left(_)    => fail("validation should have passed")
    }
  }

  test("DataSourceInput - string - valid empty") {
    val stringValid = "[]"
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(stringValid) match {
      case Right(dss) => assertEquals(dss.sources().size(), 0)
      case Left(_)    => fail("validation should have passed")
    }
  }

  test("DataSourceInput - string - multiple errors, sorted by id") {
    val stringValid =
      """
        |[{"id":"2", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"},
        |{"id":"1", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]
        |""".stripMargin
    val validator = DataSourceValidator(10, validateException)
    validator.validate(stringValid) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assertEquals(errors.size, 2)
        assert(errors.head.error.contains("validate error"))
        assertEquals(errors.head.id, "1")
        assert(errors.last.error.contains("validate error"))
        assertEquals(errors.last.id, "2")
    }
  }

  test("DataSourceInput - string - invalid json") {
    val stringInvalidJson = "[aa"
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(stringInvalidJson) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assertEquals(errors.size, 1)
        assert(errors.head.error.startsWith("failed to parse input: "))
    }
  }

  test("DataSourceInput - string - empty id") {
    val stringEmptyId =
      """[{"id":"", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(stringEmptyId) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assertEquals(errors.size, 1)
        assert(errors.head.error.startsWith("id cannot be empty"))
        assertEquals(errors.head.id, "<empty_id>")
    }
  }

  test("DataSourceInput - string - null id") {
    val stringNullId =
      """[{"id":null, "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(stringNullId) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assertEquals(errors.size, 1)
        assert(errors.head.error.startsWith("id cannot be null"))
        assertEquals(errors.head.id, "<null_id>")
    }
  }

  test("DataSourceInput - string - dup id") {
    val stringDupId =
      """
        |[{"id":"111", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}
        |,{"id":"111", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]
        |""".stripMargin
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(stringDupId) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assertEquals(errors.size, 1)
        assertEquals(errors.head.error, "id cannot be duplicated")
        assertEquals(errors.head.id, "111")
    }
  }

  test("DataSourceInput - parsed - valid") {
    val dsList = List(
      new DataSource("111", "http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s")
    )
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(dsList) match {
      case Right(dss) => assertEquals(dss.sources().size(), 1)
      case Left(_)    => fail("validation should have passed")
    }
  }

  test("DataSourceInput - parsed - dup id") {
    val dsList = List(
      new DataSource(
        "111",
        "http://local-dev/api/v1/graph?q=name,jvm.gc.pause111,:eq,:sum&step=10s"
      ),
      new DataSource(
        "111",
        "http://local-dev/api/v1/graph?q=name,jvm.gc.pause222,:eq,:sum&step=10s"
      )
    )
    val validator = DataSourceValidator(10, validateNoop)
    validator.validate(dsList) match {
      case Right(_) =>
        fail("validation should have passed")
      case Left(errors) =>
        assertEquals(errors.head.error, "id cannot be duplicated")
        assertEquals(errors.head.id, "111")
    }
  }

  test("DataSourceInput - exceed max DataSources") {
    val dsList = List(
      new DataSource(
        "111",
        "http://local-dev/api/v1/graph?q=name,jvm.gc.pause111,:eq,:sum&step=10s"
      ),
      new DataSource(
        "222",
        "http://local-dev/api/v1/graph?q=name,jvm.gc.pause222,:eq,:sum&step=10s"
      )
    )
    val validator = DataSourceValidator(1, validateNoop)
    validator.validate(dsList) match {
      case Right(_) =>
        fail("validation should have passed")
      case Left(errors) =>
        assertEquals(errors.head.error, "number of DataSources cannot exceed 1")
        assertEquals(errors.head.id, "_")
    }
  }

}
