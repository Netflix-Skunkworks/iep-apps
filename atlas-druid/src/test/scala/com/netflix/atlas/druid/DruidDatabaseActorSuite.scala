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
package com.netflix.atlas.druid

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.druid.DruidClient._
import org.scalatest.FunSuite

class DruidDatabaseActorSuite extends FunSuite {

  import DruidDatabaseActor._

  test("dimension to spec no matches") {
    val spec = toDimensionSpec("key", Query.Equal("app", "www"))
    val expected = DefaultDimensionSpec("key", "key")
    assert(spec === expected)
  }

  test("dimension to spec: equal query") {
    val spec = toDimensionSpec("app", Query.Equal("app", "www"))
    val expected = ListFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), List("www"))
    assert(spec === expected)
  }

  test("dimension to spec: in query") {
    val spec = toDimensionSpec("app", Query.In("app", List("a", "b", "c")))
    val expected =
      ListFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), List("a", "b", "c"))
    assert(spec === expected)
  }

  test("dimension to spec: regex query") {
    val spec = toDimensionSpec("app", Query.Regex("app", "www"))
    val expected = RegexFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), "^www.*")
    assert(spec === expected)
  }

  test("dimension to spec: has key query") {
    val spec = toDimensionSpec("app", Query.HasKey("app"))
    val expected = DefaultDimensionSpec("app", "app")
    assert(spec === expected)
  }

  test("dimension to spec: gt query") {
    val spec = toDimensionSpec("app", Query.GreaterThan("app", "www"))
    val expected = DefaultDimensionSpec("app", "app")
    assert(spec === expected)
  }

  test("dimension to spec: nested") {
    val spec = toDimensionSpec(
      "app",
      Query.And(
        Query.Regex("app", "www"),
        Query.And(
          Query.Equal("app", "abc"),
          Query.In("app", List("a", "b", "c"))
        )
      )
    )
    val expected = ListFilteredDimensionSpec(
      ListFilteredDimensionSpec(
        RegexFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), "^www.*"),
        List("abc")
      ),
      List("a", "b", "c")
    )
    assert(spec === expected)
  }
}
