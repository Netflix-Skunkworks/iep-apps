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

import com.netflix.atlas.core.model.ConsolidationFunction

import java.time.Duration
import java.time.Instant
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.druid.DruidClient.*
import com.netflix.atlas.eval.graph.DefaultSettings
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.webapi.GraphApi.DataRequest
import com.netflix.atlas.webapi.GraphApi.DataResponse
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.pattern.ask
import org.mockito.Mockito.mock
import org.apache.pekko.util.Timeout
import org.apache.pekko.testkit.ImplicitSender
import org.apache.pekko.testkit.TestKitBase
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.http.scaladsl.model.HttpRequest

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.util.Success
import scala.util.Try
import scala.util.Using

class DruidDatabaseActorSuite extends FunSuite with TestKitBase with ImplicitSender {

  implicit val system: ActorSystem = ActorSystem()

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
    val config = ConfigFactory.load()
    val grapher = Grapher(DefaultSettings(config))
    val uri = Uri("/api/v1/graph?q=b,foo,:eq,:sum&id=test")
    val graphCfg = grapher.toGraphConfig(uri)

    val druidQueryContext = toDruidQueryContext(
      DataRequest(
        context: EvalContext,
        List(),
        Some(graphCfg)
      )
    )
    assertEquals(druidQueryContext("atlasQuerySource"), "test")
    assertEquals(druidQueryContext("atlasQueryString"), "b,foo,:eq,:sum")
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

  test("toDruidQueries: percentile restriction") {
    val expr = DataExpr.Sum(Query.And(Query.Equal("a", "1"), Query.LessThan("percentile", "T00F2")))
    val queries = toDruidQueries(metadata, toTestDruidQueryContext(expr), context, expr)
    assertEquals(queries.size, 4)
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

  private def newDruidClient(mockResponses: List[Try[HttpResponse]]): DruidClient = {
    // Iterator to cycle through the list of results
    val resultIterator = Iterator.continually(mockResponses).flatten

    val client = Flow[(HttpRequest, AccessLogger)]
      .map {
        case (_, logger) =>
          val result = resultIterator.next() // Get the next result in the list
          result -> logger
      }

    val config = ConfigFactory.load().getConfig("atlas.druid")
    new DruidClient(config, system, client)
  }

  // This helper function is needed since two NaNs are not equivalent in scala
  def assertArraysAreEqualWithNaN(arr1: Array[Double], arr2: Array[Double]): Unit = {
    assertEquals(arr1.length, arr2.length, "Arrays have different lengths")

    arr1.indices.foreach { i =>
      val (a, b) = (arr1(i), arr2(i))
      if (!a.isNaN || !b.isNaN) {
        assertEquals(a, b, s"Arrays differ at index $i: $a != $b")
      }
    }
  }

  def getTimeseriesDataPoints(ts: TimeSeries, start: Long, end: Long): Array[Double] = {
    val datapointBuffer = ArrayBuffer[Double]()
    ts.data.foreach(start - 1, end) { (_: Long, datapoint: Double) =>
      {
        datapointBuffer += datapoint
      }
    }
    datapointBuffer.toArray
  }

  def metadataWithDifferentStepSizes(): Metadata = {
    Metadata(
      List(
        DatasourceMetadata(
          "my_datasource_5s",
          Datasource(
            List("x", "y"),
            List(Metric("my_metric_5s", "LONG", primaryStep = 5000))
          )
        ),
        DatasourceMetadata(
          "my_datasource",
          Datasource(
            List("x", "y"),
            List(Metric("my_metric", "LONG")) // defaults to 60_000 secs
          )
        )
      )
    )
  }

  def runDataRequestQueryTest(
    files: List[String],
    query: String,
    start: Long,
    end: Long,
    requestStepSize: Long
  ): TimeSeries = {
    import com.netflix.atlas.core.util.Streams.*
    val config = ConfigFactory.load()
    val dataExpr = DataExpr.Sum(evalQuery(query))

    val service = mock(classOf[DruidMetadataService])

    val mockResponses = files.map(f => {
      val payload = Using.resource(resource(f))(byteArray)
      val response = HttpResponse(StatusCodes.OK, entity = payload)
      Success(response)
    })
    val druidClient = newDruidClient(mockResponses)

    val actorRef: ActorRef =
      system.actorOf(Props(new DruidDatabaseActor(config, service, druidClient)))

    implicit val timeout: Timeout = Timeout(30.seconds) // Define implicit timeout
    val futureResponse = (actorRef ? metadataWithDifferentStepSizes()).mapTo[String]
    val result: String = Await.result(futureResponse, timeout.duration)
    assertEquals(result, "metadata_updated")

    val grapher = Grapher(DefaultSettings(config))
    val uri = Uri(f"/api/v1/graph?q=$query&id=test")
    val graphCfg = grapher.toGraphConfig(uri)

    val evalContext = EvalContext(
      start, // start
      end, // end
      requestStepSize // step, this should be ignored
    )

    val dataRequest: DataRequest = DataRequest(
      evalContext,
      List(dataExpr),
      Some(graphCfg)
    )
    val future = actorRef ? dataRequest

    val dataResponse: DataResponse =
      Await.result(future.mapTo[DataResponse], Timeout(30.seconds).duration)

    val tsList: List[TimeSeries] = dataResponse.ts(dataExpr)
    assertEquals(tsList.size, 1)
    tsList.head
  }

  test("querying a 60s datasource with a 60s step request should respond as 60s") {
    val start = 1746805800000L
    val end = 1746806405000L
    val requestStepSize = 60000L
    val ts: TimeSeries =
      runDataRequestQueryTest(
        List("timeseriesResponse60sStep.json"),
        "name,my_metric,:eq",
        start,
        end,
        requestStepSize
      )

    assertEquals(ts.label, "name=my_metric")
    assertEquals(ts.tags, Map("name" -> "my_metric"))
    // Always use the maximum step of the requestedStepSize and the metrics.
    assertEquals(ts.data.step, requestStepSize)

    assertArraysAreEqualWithNaN(
      getTimeseriesDataPoints(ts, start, end),
      Array(
        Double.NaN,
        Double.NaN,
        25.366666666666667,
        28.233333333333334,
        50.666666666666664,
        27.266666666666666,
        21.95,
        27.916666666666668,
        25.783333333333335,
        20.783333333333335,
        22.633333333333333
      )
    )
  }

  test("querying a 60s datasource with a 5s step request should respond as 60s") {
    val start = 1746805800000L
    val end = 1746806405000L
    val requestStepSize = 5000L
    val ts: TimeSeries =
      runDataRequestQueryTest(
        List("timeseriesResponse60sStep.json"),
        "name,my_metric,:eq",
        start,
        end,
        requestStepSize
      )

    assertEquals(ts.label, "name=my_metric")
    assertEquals(ts.tags, Map("name" -> "my_metric"))
    // Always use the maximum step of the requestedStepSize and the metrics.
    assertEquals(ts.data.step, 60000L)

    assertArraysAreEqualWithNaN(
      getTimeseriesDataPoints(ts, start, end),
      Array(
        Double.NaN,
        Double.NaN,
        25.366666666666667,
        28.233333333333334,
        50.666666666666664,
        27.266666666666666,
        21.95,
        27.916666666666668,
        25.783333333333335,
        20.783333333333335,
        22.633333333333333
      )
    )
  }

  test("querying a 5s datasource with a 60s step request should respond as 60s") {
    val start = 1746805800000L
    val end = 1746806405000L
    val requestStepSize = 60000L
    val ts: TimeSeries =
      runDataRequestQueryTest(
        List("timeseriesResponse5sStep.json"),
        "name,my_metric_5s,:eq",
        start,
        end,
        requestStepSize
      )

    assertEquals(ts.label, "name=my_metric_5s")
    assertEquals(ts.tags, Map("name" -> "my_metric_5s"))
    // Always use the maximum step of the requestedStepSize and the metrics.
    assertEquals(ts.data.step, requestStepSize)

    assertArraysAreEqualWithNaN(
      getTimeseriesDataPoints(ts, start, end),
      Array(
        Double.NaN,
        Double.NaN,
        0.48333333333333334,
        0.65,
        0.55,
        0.5333333333333333,
        0.7,
        0.7166666666666667,
        0.4666666666666667,
        0.6333333333333333,
        0.45
      )
    )
  }

  test("querying a 5s datasource with a 5s step request should respond as 5s") {
    val start = 1746805800000L
    val end = 1746806405000L
    val requestStepSize = 5000L
    val ts: TimeSeries =
      runDataRequestQueryTest(
        List("timeseriesResponse5sStep.json"),
        "name,my_metric_5s,:eq",
        start,
        end,
        requestStepSize
      )

    assertEquals(ts.label, "name=my_metric_5s")
    assertEquals(ts.tags, Map("name" -> "my_metric_5s"))
    // Always use the maximum step of the requestedStepSize and the metrics.
    assertEquals(ts.data.step, requestStepSize)

    assertArraysAreEqualWithNaN(
      getTimeseriesDataPoints(ts, start, end),
      Array(
        Double.NaN,
        Double.NaN,
        9.8,
        9.0,
        6.0,
        7.4,
        9.4,
        7.4,
        8.4,
        8.6,
        7.4,
        7.0,
        9.6,
        5.8,
        7.6,
        5.0,
        6.4,
        7.0,
        6.4,
        5.8,
        7.2,
        7.0,
        7.8,
        6.2,
        7.4,
        7.8,
        8.0,
        8.2,
        5.4,
        5.8,
        7.6,
        8.2,
        7.2,
        8.6,
        7.0,
        5.6,
        6.2,
        6.6,
        6.4,
        7.2,
        7.4,
        7.8,
        7.4,
        6.6,
        6.0,
        7.4,
        6.6,
        7.4,
        6.4,
        6.4,
        5.2,
        6.2,
        9.0,
        5.2,
        7.0,
        8.2,
        6.4,
        6.0,
        8.8,
        8.6,
        7.2,
        8.4,
        5.4,
        8.0,
        7.0,
        7.2,
        6.6,
        7.6,
        7.4,
        7.8,
        7.0,
        7.0,
        9.6,
        8.6,
        7.4,
        6.8,
        8.0,
        6.4,
        6.6,
        6.4,
        8.4,
        6.4,
        6.6,
        7.4,
        6.4,
        5.6,
        5.2,
        8.0,
        8.6,
        7.6,
        6.4,
        7.4,
        6.0,
        8.0,
        6.2,
        9.6,
        8.2,
        7.6,
        6.2,
        5.4,
        6.2,
        8.2,
        9.0,
        6.4,
        8.6,
        5.6,
        6.8,
        7.6,
        7.4,
        5.4,
        5.4,
        6.8,
        5.6,
        6.4,
        6.8,
        7.2,
        9.0,
        8.8,
        8.0,
        5.8,
        6.8,
        6.8
      )
    )
  }

  test("querying 60s and 5s datasources with a 60s step request should respond as 60s") {
    val start = 1746805800000L
    val end = 1746806405000L
    val requestStepSize = 60000L
    val ts: TimeSeries = runDataRequestQueryTest(
      List("timeseriesResponse5sStep.json", "timeseriesResponse60sStep.json"),
      "name,(,my_metric,my_metric_5s,),:in",
      start,
      end,
      requestStepSize
    )

    assertEquals(ts.label, "name=unknown")
    assertEquals(ts.tags, Map("name" -> "unknown"))
    // Always use the maximum step of the requestedStepSize and the metrics.
    assertEquals(ts.data.step, 60000L)

    assertArraysAreEqualWithNaN(
      getTimeseriesDataPoints(ts, start, end),
      Array(
        Double.NaN,
        Double.NaN,
        25.85,
        28.883333333333333,
        51.21666666666666,
        27.8,
        22.65,
        28.633333333333333,
        26.25,
        21.416666666666668,
        23.083333333333332
      )
    )
  }

  test("querying 60s and 5s datasources with a 5s step request should respond as 60s") {
    val start = 1746805800000L
    val end = 1746806405000L
    val requestStepSize = 5000L
    val ts: TimeSeries = runDataRequestQueryTest(
      List("timeseriesResponse5sStep.json", "timeseriesResponse60sStep.json"),
      "name,(,my_metric,my_metric_5s,),:in",
      start,
      end,
      requestStepSize
    )

    assertEquals(ts.label, "name=unknown")
    assertEquals(ts.tags, Map("name" -> "unknown"))
    // Always use the maximum step of the requestedStepSize and the metrics.
    assertEquals(ts.data.step, 60000L)

    assertArraysAreEqualWithNaN(
      getTimeseriesDataPoints(ts, start, end),
      Array(
        Double.NaN,
        Double.NaN,
        25.85,
        28.883333333333333,
        51.21666666666666,
        27.8,
        22.65,
        28.633333333333333,
        26.25,
        21.416666666666668,
        23.083333333333332
      )
    )
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
    val multiple = 300000 / 5000
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

  test("validate: simple expr with name") {
    val query = evalQuery("app,www,:eq,name,cpu,:eq,:and")
    validate(query)
  }

  test("validate: simple expr without name") {
    val query = evalQuery("app,www,:eq,not_name,cpu,:eq,:and")
    intercept[IllegalArgumentException] {
      validate(query)
    }
  }

  test("validate: OR with name") {
    val query = evalQuery("app,www,:eq,name,cpu,:eq,:and,name,disk,:eq,:or")
    validate(query)
  }

  test("validate: OR one side without name") {
    val query = evalQuery("app,www,:eq,not_name,cpu,:eq,:and,name,disk,:eq,:or")
    intercept[IllegalArgumentException] {
      validate(query)
    }
  }

  test("validate: name only in NOT") {
    val query = evalQuery("app,www,:eq,name,cpu,:eq,:not,:and")
    intercept[IllegalArgumentException] {
      validate(query)
    }
  }

  test("validate: IN name") {
    val query = evalQuery("app,www,:eq,name,(,cpu,disk,),:in,:and")
    validate(query)
  }

  test("validate: haskey name") {
    val query = evalQuery("app,www,:eq,name,:has,:and")
    validate(query)
  }

  test("validate: regex name") {
    val query = evalQuery("app,www,:eq,name,cpu,:re,:and")
    validate(query)
  }

  test("validate: greater than name") {
    val query = evalQuery("app,www,:eq,name,cpu,:gt,:and")
    validate(query)
  }
}
