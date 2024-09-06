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
package com.netflix.atlas.druid

import java.time.Duration
import java.time.Instant
import com.netflix.atlas.core.model.ConsolidationFunction
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.druid.DruidClient.*
import com.netflix.atlas.eval.graph.GraphConfig
import com.netflix.atlas.json.Json
import com.netflix.atlas.webapi.GraphApi.DataRequest
import munit.FunSuite
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when

class DruidDatabaseActorSuite extends FunSuite {

  import DruidDatabaseActor.*

  test("dimension to spec no matches") {
    val spec = toDimensionSpec("key", Query.Equal("app", "www"))
    val expected = DefaultDimensionSpec("key", "key")
    assertEquals(spec, expected)
  }

  test("dimension to spec: equal query") {
    val spec = toDimensionSpec("app", Query.Equal("app", "www"))
    val expected = ListFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), List("www"))
    assertEquals(spec, expected)
  }

  test("dimension to spec: in query") {
    val spec = toDimensionSpec("app", Query.In("app", List("a", "b", "c")))
    val expected =
      ListFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), List("a", "b", "c"))
    assertEquals(spec, expected)
  }

  test("dimension to spec: regex query") {
    val spec = toDimensionSpec("app", Query.Regex("app", "www"))
    val expected = RegexFilteredDimensionSpec(DefaultDimensionSpec("app", "app"), "^www.*")
    assertEquals(spec, expected)
  }

  test("dimension to spec: has key query") {
    val spec = toDimensionSpec("app", Query.HasKey("app"))
    val expected = DefaultDimensionSpec("app", "app")
    assertEquals(spec, expected)
  }

  test("dimension to spec: gt query") {
    val spec = toDimensionSpec("app", Query.GreaterThan("app", "www"))
    val expected = DefaultDimensionSpec("app", "app")
    assertEquals(spec, expected)
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
    assertEquals(spec, expected)
  }

  test("exactTags: extract equal clauses") {
    val expected = Map("a" -> "1", "b" -> "2")
    val query = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    assertEquals(exactTags(query), expected)
  }

  test("exactTags: ignore regex") {
    val expected = Map("b" -> "2")
    val query = Query.And(Query.Regex("a", "1"), Query.Equal("b", "2"))
    assertEquals(exactTags(query), expected)
  }

  test("exactTags: ignore OR subtree") {
    val expected = Map("a" -> "1")
    val query =
      Query.And(Query.Equal("a", "1"), Query.Or(Query.Equal("b", "2"), Query.Equal("c", "3")))
    assertEquals(exactTags(query), expected)
  }

  test("toAggregation: sum") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.sum("test"))
  }

  test("toAggregation: count") {
    val expr = DataExpr.Count(Query.Equal("a", "1"))
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.count("test"))
  }

  test("toAggregation: min") {
    val expr = DataExpr.Min(Query.Equal("a", "1"))
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.min("test"))
  }

  test("toAggregation: max") {
    val expr = DataExpr.Max(Query.Equal("a", "1"))
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.max("test"))
  }

  test("toAggregation: sum grouped") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("a", "1")), List("a"))
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.sum("test"))
  }

  test("toAggregation: max grouped") {
    val expr = DataExpr.GroupBy(DataExpr.Max(Query.Equal("a", "1")), List("a"))
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.max("test"))
  }

  test("toAggregation: sum grouped max cf") {
    val expr = DataExpr.GroupBy(
      DataExpr.Consolidation(DataExpr.Sum(Query.Equal("a", "1")), ConsolidationFunction.Max),
      List("a")
    )
    val aggr = toAggregation(DruidClient.Metric("test"), expr)
    assertEquals(aggr, Aggregation.sum("test"))
  }

  test("toAggregation: all") {
    intercept[UnsupportedOperationException] {
      val expr = DataExpr.All(Query.Equal("a", "1"))
      toAggregation(DruidClient.Metric("test"), expr)
    }
  }

  test("getDimensions: simple sum") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("a", "1")), List("a"))
    val expected = List(
      ListFilteredDimensionSpec(
        DefaultDimensionSpec("a", "a"),
        List("1")
      )
    )
    assertEquals(getDimensions(expr), expected)
  }

  test("getDimensions: regex sum") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Regex("a", "1")), List("a"))
    val expected = List(
      RegexFilteredDimensionSpec(
        DefaultDimensionSpec("a", "a"),
        "^1.*"
      )
    )
    assertEquals(getDimensions(expr), expected)
  }

  test("getDimensions: has sum") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.HasKey("a")), List("a"))
    val expected = List(DefaultDimensionSpec("a", "a"))
    assertEquals(getDimensions(expr), expected)
  }

  test("getDimensions: greater-than sum") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.GreaterThan("a", "1")), List("a"))
    val expected = List(DefaultDimensionSpec("a", "a"))
    assertEquals(getDimensions(expr), expected)
  }

  test("getDimensions: regex sum ignore special") {
    val expr =
      DataExpr.GroupBy(DataExpr.Sum(Query.Regex("nf.datasource", "1")), List("nf.datasource"))
    val expected = List.empty[DimensionSpec]
    assertEquals(getDimensions(expr), expected)
  }

  private val metadata = Metadata(
    List(
      DatasourceMetadata(
        "ds_1",
        Datasource(
          List("a", "b"),
          List(Metric("m1", "LONG"), Metric("m2", "LONG"))
        )
      ),
      DatasourceMetadata(
        "ds_2",
        Datasource(
          List("a", "c"),
          List(Metric("m1", "LONG"), Metric("m3", "LONG"))
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

  private def toTestDruidQueryContext(expr: DataExpr): Map[String, String] = {
    Map(
      "atlasQuerySource" -> "test",
      "atlasQueryString" -> expr.toString(),
      "atlasQueryGuid"   -> "test-query-guid"
    )
  }

  test("toDruidQueryContext: should produce valid map from request") {
    val mockConfig = mock[GraphConfig]
    // Stub the id and query fields
    when(mockConfig.id).thenReturn("mocked-id")
    when(mockConfig.query).thenReturn("b,foo,:eq,:sum b,bar,:eq,:sum")

    val druidQueryContext = toDruidQueryContext(
      DataRequest(
        context: EvalContext,
        List(),
        Some(mockConfig)
      )
    )
    assertEquals(druidQueryContext("atlasQuerySource"), "mocked-id")
    assertEquals(druidQueryContext("atlasQueryString"), "b,foo,:eq,:sum b,bar,:eq,:sum")
    assert(druidQueryContext("atlasQueryGuid").nonEmpty)
  }

  test("toDruidQueries: simple sum") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val queries = toDruidQueries(metadata, toTestDruidQueryContext(expr), context, expr)

    assertEquals(queries.size, 4)
    assertEquals(queries.map(_._1("name")).toSet, Set("m1", "m2", "m3"))
    assertEquals(queries.map(_._1("nf.datasource")).toSet, Set("ds_1", "ds_2"))
    queries.forall(_._3.asInstanceOf[TimeseriesQuery].context == toTestDruidQueryContext(expr))
  }

  test("toDruidQueries: unknown dimensions") {
    val expr = DataExpr.Sum(Query.HasKey("c"))
    val queries = toDruidQueries(metadata, toTestDruidQueryContext(expr), context, expr)

    assertEquals(queries.size, 2)
    assertEquals(queries.map(_._1("name")).toSet, Set("m1", "m3"))
    assertEquals(queries.map(_._1("nf.datasource")).toSet, Set("ds_2"))
    queries.forall(_._3.asInstanceOf[TimeseriesQuery].context == toTestDruidQueryContext(expr))
  }

  test("toDruidQueries: unknown dimensions missing") {
    val expr = DataExpr.Sum(Query.Not(Query.HasKey("c")))
    val queries = toDruidQueries(metadata, toTestDruidQueryContext(expr), context, expr)

    assertEquals(queries.size, 4)
    assertEquals(queries.map(_._1("name")).toSet, Set("m1", "m2", "m3"))
    assertEquals(queries.map(_._1("nf.datasource")).toSet, Set("ds_1", "ds_2"))
    queries.forall(_._3.asInstanceOf[TimeseriesQuery].context == toTestDruidQueryContext(expr))
  }

  test("toDruidQueries: or with one missing dimension") {
    val expr = DataExpr.Sum(Query.Or(Query.Equal("a", "1"), Query.Equal("d", "2")))
    val queries = toDruidQueries(metadata, toTestDruidQueryContext(expr), context, expr)
    assert(queries.forall(_._1.contains("a")))
    queries.forall(_._3.asInstanceOf[TimeseriesQuery].context == toTestDruidQueryContext(expr))
  }

  test("toDruidQueries: or with name dimension") {
    val expr = DataExpr.Sum(
      Query.Or(
        Query.And(
          Query.Equal("name", "m1"),
          Query.Equal("b", "foo")
        ),
        Query.Equal("name", "m3")
      )
    )
    val queries = toDruidQueries(metadata, toTestDruidQueryContext(expr), context, expr)
    val m1 = queries.filter(t => t._2.name == "m1").head
    val json = Json.encode(m1._3)
    assert(json.contains("""{"dimension":"b","value":"foo","type":"selector"}"""))
    queries.forall(_._3.asInstanceOf[TimeseriesQuery].context == toTestDruidQueryContext(expr))
  }

  private def evalQuery(str: String): Query = {
    Interpreter(QueryVocabulary.allWords).execute(str).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalArgumentException("invalid expression")
    }
  }

  test("simplify: single name with or") {
    val q = evalQuery("name,a,:eq,dim,1,:eq,:and,name,b,:eq,dim,2,:eq,:and,:or")
    val expected = evalQuery("dim,1,:eq")
    assertEquals(simplify(q, List("a"), List("dim")), expected)
  }

  test("simplify: multiple names with or") {
    val q = evalQuery("name,a,:eq,dim,1,:eq,:and,name,b,:eq,dim,2,:eq,:and,:or")
    val expected = evalQuery("dim,1,:eq,dim,2,:eq,:or")
    assertEquals(simplify(q, List("a", "b"), List("dim")), expected)
  }

  test("simplifyExact: conjunctive clause") {
    val q = evalQuery("a,1,:eq,b,2,:eq,:and,c,3,:eq,:and")
    assertEquals(simplifyExact(q), q)
  }

  test("simplifyExact: not eq clause") {
    val q = evalQuery("a,1,:eq,b,2,:eq,:and,c,3,:eq,:not,:and")
    val expected = evalQuery("a,1,:eq,b,2,:eq,:and")
    assertEquals(simplifyExact(q), expected)
  }

  test("simplifyExact: not re clause") {
    val q = evalQuery("a,1,:eq,b,2,:eq,:and,c,3,:re,:not,:and")
    val expected = evalQuery("a,1,:eq,b,2,:eq,:and")
    assertEquals(simplifyExact(q), expected)
  }

  test("simplifyExact: or clause") {
    val q = evalQuery("a,1,:eq,a,2,:eq,:or")
    assertEquals(simplifyExact(q), Query.True)
  }

  test("simplifyExact: or query as part of conjunctive clause") {
    val q = evalQuery("a,1,:eq,b,2,:eq,c,2,:eq,:or,:and")
    val expected = evalQuery("a,1,:eq")
    assertEquals(simplifyExact(q), expected)
  }

  test("createValueMapper: normalize rates, sum") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val mapper = createValueMapper(true, context, expr)
    assertEquals(mapper(1.0), 1.0 / 60)
  }

  test("createValueMapper: normalize rates, max") {
    val expr = DataExpr.Max(Query.Equal("a", "1"))
    val mapper = createValueMapper(true, context, expr)
    assertEquals(mapper(1.0), 1.0 / 60)
  }

  test("createValueMapper: avg consolidation") {
    val expr = DataExpr.Sum(Query.Equal("a", "1"))
    val mapper = createValueMapper(false, context.copy(step = 300000), expr)
    val multiple = 300000 / DefaultSettings.stepSize
    assertEquals(mapper(1.0), 1.0 / multiple)
  }

  test("createValueMapper: max consolidation") {
    val expr = DataExpr.Max(Query.Equal("a", "1"))
    val mapper = createValueMapper(false, context.copy(step = 300000), expr)
    assertEquals(mapper(1.0), 1.0)
  }

  test("createValueMapper: min consolidation") {
    val expr = DataExpr.Min(Query.Equal("a", "1"))
    val mapper = createValueMapper(false, context.copy(step = 300000), expr)
    assertEquals(mapper(1.0), 1.0)
  }

  test("createValueMapper: sum consolidation") {
    val expr = DataExpr.Sum(Query.Equal("a", "1")).withConsolidation(ConsolidationFunction.Sum)
    val mapper = createValueMapper(false, context.copy(step = 300000), expr)
    assertEquals(mapper(1.0), 1.0)
  }
}
