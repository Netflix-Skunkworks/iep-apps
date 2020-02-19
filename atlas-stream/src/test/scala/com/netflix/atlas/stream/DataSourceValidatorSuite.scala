/*
 * Copyright 2014-2020 Netflix, Inc.
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
import org.scalatest.funsuite.AnyFunSuite

class DataSourceValidatorSuite extends AnyFunSuite {

  private val validateNoop: DataSource => Unit = _ => ()
  private val validateException: DataSource => Unit = _ => { throw new Exception("validate error") }

  test("DataSourceInput - string - valid") {
    val stringValid =
      """[{"id":"abc", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    DataSourceValidator.validate(stringValid, validateNoop) match {
      case Right(dss) => assert(dss.getSources.size() === 1)
      case Left(_)    => fail("validation should have passed")
    }
  }

  test("DataSourceInput - string - valid empty") {
    val stringValid = "[]"
    DataSourceValidator.validate(stringValid, validateNoop) match {
      case Right(dss) => assert(dss.getSources.size() === 0)
      case Left(_)    => fail("validation should have passed")
    }
  }

  test("DataSourceInput - string - multiple errors, sorted by id") {
    val stringValid =
      """
        |[{"id":"2", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"},
        |{"id":"1", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]
        |""".stripMargin
    DataSourceValidator.validate(stringValid, validateException) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assert(errors.size === 2)
        assert(errors.head.error.contains("validate error"))
        assert(errors.head.id === "1")
        assert(errors.last.error.contains("validate error"))
        assert(errors.last.id === "2")
    }
  }

  test("DataSourceInput - string - invalid json") {
    val stringInvalidJson = "[aa"
    DataSourceValidator.validate(stringInvalidJson, validateNoop) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assert(errors.size === 1)
        assert(errors.head.error.startsWith("failed to parse input: "))
    }
  }

  test("DataSourceInput - string - empty id") {
    val stringEmptyId =
      """[{"id":"", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    DataSourceValidator.validate(stringEmptyId, validateNoop) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assert(errors.size === 1)
        assert(errors.head.error.startsWith("id cannot be empty"))
        assert(errors.head.id === "<empty_id>")
    }
  }

  test("DataSourceInput - string - null id") {
    val stringNullId =
      """[{"id":null, "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    DataSourceValidator.validate(stringNullId, validateNoop) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assert(errors.size === 1)
        assert(errors.head.error.startsWith("id cannot be null"))
        assert(errors.head.id === "<null_id>")
    }
  }

  test("DataSourceInput - string - dup id") {
    val stringDupId =
      """
        |[{"id":"111", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}
        |,{"id":"111", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]
        |""".stripMargin
    DataSourceValidator.validate(stringDupId, validateNoop) match {
      case Right(_) =>
        fail("validation should have failed")
      case Left(errors) =>
        assert(errors.size === 1)
        assert(errors.head.error === "id cannot be duplicated")
        assert(errors.head.id === "111")
    }
  }

  test("DataSourceInput - parsed - valid") {
    val dsList = List(
      new DataSource("111", "http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s")
    )
    DataSourceValidator.validate(dsList, validateNoop) match {
      case Right(dss) => assert(dss.getSources.size() === 1)
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
    DataSourceValidator.validate(dsList, validateNoop) match {
      case Right(_) =>
        fail("validation should have passed")
      case Left(errors) =>
        assert(errors.head.error === "id cannot be duplicated")
        assert(errors.head.id === "111")
    }
  }

}
