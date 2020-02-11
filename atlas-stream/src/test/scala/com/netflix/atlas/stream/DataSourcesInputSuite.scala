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

class DataSourcesInputSuite extends AnyFunSuite {

  private val validateNoop: DataSource => Unit = _ => ()
  private val validateException: DataSource => Unit = _ => { throw new Exception("validate error") }

  test("DataSourceInput - string - valid") {
    val stringValid =
      """[{"id":"abc", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    val dsInput = new DataSourceInput(stringValid, validateNoop)
    assert(dsInput.isValid)
    assert(dsInput.errors.size === 0)
    assert(dsInput.dataSources.getSources.size() == 1)
  }

  test("DataSourceInput - string - valid empty") {
    val stringValid = "[]"
    val dsInput = new DataSourceInput(stringValid, validateNoop)
    assert(dsInput.isValid)
    assert(dsInput.errors.size === 0)
    assert(dsInput.dataSources.getSources.size() == 0)
  }

  test("DataSourceInput - string - multiple errors, sorted by id") {
    val stringValid =
      """
        |[{"id":"2", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"},
        |{"id":"1", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]
        |""".stripMargin
    val dsInput = new DataSourceInput(stringValid, validateException)
    assert(!dsInput.isValid)
    assert(dsInput.errors.size === 2)
    assert(dsInput.errors.head.error.contains("validate error"))
    assert(dsInput.errors.head.id === "1")
    assert(dsInput.errors.last.error.contains("validate error"))
    assert(dsInput.errors.last.id === "2")

  }

  test("DataSourceInput - string - invalid json") {
    val stringInvalidJson = "["
    val dsInput = new DataSourceInput(stringInvalidJson, validateNoop)
    assert(!dsInput.isValid)
    assert(dsInput.errors.size === 1)
    assert(dsInput.errors.head.error.startsWith("failed to parse input as List[DataSource]: "))
  }

  test("DataSourceInput - string - empty id") {
    val stringEmptyId =
      """[{"id":"", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    val dsInput = new DataSourceInput(stringEmptyId, validateNoop)
    assert(!dsInput.isValid)
    assert(dsInput.errors.size === 1)
    assert(dsInput.errors.head.error.startsWith("id cannot be empty"))
    assert(dsInput.errors.head.id === "<empty_id>")
  }

  test("DataSourceInput - string - null id") {
    val stringNullId =
      """[{"id":null, "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]"""
    val dsInput = new DataSourceInput(stringNullId, validateNoop)
    assert(!dsInput.isValid)
    assert(dsInput.errors.size === 1)
    assert(dsInput.errors.head.error.startsWith("id cannot be null"))
    assert(dsInput.errors.head.id === "<null_id>")
  }

  test("DataSourceInput - string - dup id") {
    val stringDupId =
      """
        |[{"id":"111", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}
        |,{"id":"111", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s"}]
        |""".stripMargin
    val dsInput = new DataSourceInput(stringDupId, validateNoop)
    assert(!dsInput.isValid)
    assert(dsInput.errors.size === 1)
    assert(dsInput.errors.head.error === "id cannot be duplicated")
    assert(dsInput.errors.head.id === "111")
  }

  test("DataSourceInput - parsed - valid") {
    val dsList = List(
      new DataSource("111", "http://local-dev/api/v1/graph?q=name,jvm.gc.pause,:eq,:sum&step=10s")
    )
    val dsInput = new DataSourceInput(dsList, validateNoop)
    assert(dsInput.isValid)
    assert(dsInput.dataSources.getSources.size() === 1)
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
    val dsInput = new DataSourceInput(dsList, validateNoop)
    assert(!dsInput.isValid)
    assert(dsInput.errors.head.error === "id cannot be duplicated")
    assert(dsInput.errors.head.id === "111")
  }

}
