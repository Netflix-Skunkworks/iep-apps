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
package com.netflix.atlas.druid

import java.time.Duration
import java.time.Instant

import com.netflix.atlas.core.model.ConsolidationFunction
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
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

  test("exactTags: extract equal clauses") {
    val expected = Map("a" -> "1", "b" -> "2")
    val query = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    assert(exactTags(query) === expected)
  }

  test("exactTags: ignore regex") {
    val expected = Map("b" -> "2")
    val query = Query.And(Query.Regex("a", "1"), Query.Equal("b", "2"))
    assert(exactTags(query) === expected)
  }

  test("exactTags: ignore OR subtree") {
    val expected = Map("a" -> "1")
    val query =
      Query.And(Query.Equal("a", "1"), Query.Or(Query.Equal("b", "2"), Query.Equal("c", "3")))
    assert(exactTags(query) === expected)
  }

  test("rangeKeys: ignore equal clauses") {
    val expected = Set.empty[String]
    val query = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    assert(rangeKeys(query) === expected)
  }

  test("rangeKeys: extract regex key") {
    val expected = Set("a")
    val query = Query.And(Query.Regex("a", "1"), Query.Equal("b", "2"))
    assert(rangeKeys(query) === expected)
  }

  test("rangeKeys: extract gt key") {
    val expected = Set("a")
    val query = Query.And(Query.GreaterThan("a", "1"), Query.Equal("b", "2"))
    assert(rangeKeys(query) === expected)
  }

  test("rangeKeys: extract in key") {
    val expected = Set("a")
    val query = Query.And(Query.In("a", List("1", "2")), Query.Equal("b", "2"))
    assert(rangeKeys(query) === expected)
  }

  test("rangeKeys: extract has key") {
    val expected = Set("a")
    val query = Query.And(Query.HasKey("a"), Query.Equal("b", "2"))
    assert(rangeKeys(query) === expected)
  }

  test("rangeKeys: extract keys in OR subtree") {
    val expected = Set("b", "c")
    val query = Query.And(
      Query.Equal("a", "1"),
      Query.Or(
        Query.Equal("b", "2"),
        Query.HasKey("c")
      )
    )
    assert(rangeKeys(query) === expected)
  }

  test("toAggregation: sum") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.sum("test"))
  }

  test("toAggregation: count") {
    val expr = DataExpr.Count(Query.Equal("a", "1"))
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.count("test"))
  }

  test("toAggregation: min") {
    val expr = DataExpr.Min(Query.Equal("a", "1"))
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.min("test"))
  }

  test("toAggregation: max") {
    val expr = DataExpr.Max(Query.Equal("a", "1"))
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.max("test"))
  }

  test("toAggregation: sum grouped") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("a", "1")), List("a"))
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.sum("test"))
  }

  test("toAggregation: max grouped") {
    val expr = DataExpr.GroupBy(DataExpr.Max(Query.Equal("a", "1")), List("a"))
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.max("test"))
  }

  test("toAggregation: sum grouped max cf") {
    val expr = DataExpr.GroupBy(
      DataExpr.Consolidation(DataExpr.Sum(Query.Equal("a", "1")), ConsolidationFunction.Max),
      List("a")
    )
    val aggr = toAggregation("test", expr)
    assert(aggr === Aggregation.sum("test"))
  }

  test("toAggregation: all") {
    intercept[UnsupportedOperationException] {
      val expr = DataExpr.All(Query.Equal("a", "1"))
      toAggregation("test", expr)
    }
  }

  test("getDimensions: simple sum") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val expected = List.empty[DimensionSpec]
    assert(getDimensions(expr) === expected)
  }

  test("getDimensions: regex sum") {
    val expr = DataExpr.Sum(Query.Regex("a", "1"))
    val expected = List(
      RegexFilteredDimensionSpec(
        DefaultDimensionSpec("a", "a"),
        "^1.*"
      )
    )
    assert(getDimensions(expr) === expected)
  }

  test("getDimensions: has sum") {
    val expr = DataExpr.Sum(Query.HasKey("a"))
    val expected = List(DefaultDimensionSpec("a", "a"))
    assert(getDimensions(expr) === expected)
  }

  test("getDimensions: greater-than sum") {
    val expr = DataExpr.Sum(Query.GreaterThan("a", "1"))
    val expected = List(DefaultDimensionSpec("a", "a"))
    assert(getDimensions(expr) === expected)
  }

  test("getDimensions: regex sum ignore special") {
    val expr = DataExpr.Sum(Query.Regex("nf.datasource", "1"))
    val expected = List.empty[DimensionSpec]
    assert(getDimensions(expr) === expected)
  }

  private val metadata = Metadata(
    List(
      DatasourceMetadata(
        "ds_1",
        Datasource(
          List("a", "b"),
          List("m1", "m2")
        )
      ),
      DatasourceMetadata(
        "ds_2",
        Datasource(
          List("a", "c"),
          List("m1", "m3")
        )
      )
    )
  )

  private val endTime = Instant.parse("2019-01-10T12:00:00Z")

  private val context = EvalContext(
    endTime.minus(Duration.ofHours(1)).toEpochMilli,
    endTime.toEpochMilli,
    Duration.ofMinutes(1).toMillis
  )

  test("toDruidQueries: simple sum") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val queries = toDruidQueries(metadata, context, expr)

    assert(queries.size === 4)
    assert(queries.map(_._1("name")).toSet === Set("m1", "m2", "m3"))
    assert(queries.map(_._1("nf.datasource")).toSet === Set("ds_1", "ds_2"))
  }
}
