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
package com.netflix.atlas.webapi

import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods.POST
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import com.fasterxml.jackson.core.io.JsonEOFException
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.CWDP
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.timestamp
import com.netflix.atlas.cloudwatch.FirehoseMetric
import com.netflix.atlas.core.util.FastGzipOutputStream
import com.netflix.atlas.json.Json
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpoint.decodeMetricJson
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpointSuite.generateRequest
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpointSuite.makeFirehosePayload
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpointSuite.makeJson
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpointSuite.parsedDatapointsA
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpointSuite.parsedDatapointsB
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import junit.framework.TestCase.assertNull
import org.mockito.ArgumentMatchersSugar.anyLong
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.never
import org.mockito.MockitoSugar.times
import org.mockito.MockitoSugar.verify
import org.mockito.captor.ArgCaptor
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.Base64
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Using

class CloudWatchFirehoseEndpointSuite extends MUnitRouteSuite {

  var registry: Registry = null
  var processor: CloudWatchMetricsProcessor = null
  var processorCaptor = ArgCaptor[List[FirehoseMetric]]

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    processor = mock[CloudWatchMetricsProcessor]
    processorCaptor = ArgCaptor[List[FirehoseMetric]]
  }

  test("firehose success") {
    val req = generateRequest("/api/v1/firehose")
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assert(StatusCodes.OK == status)
      assertParse()
      assertCounters(3)
      assertResponse(response)
    }
  }

  test("firehose GZIP success") {
    val req = generateRequest("/api/v1/firehose", gzip = true)
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assert(StatusCodes.OK == status)
      assertParse()
      assertCounters(3)
      assertResponse(response)
    }
  }

  test("firehose success w attributes") {
    val req = generateRequest("/api/v1/firehose", attributes = Map("MyKey" -> "MyVal"))
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assert(StatusCodes.OK == status)
      assertParse(attributes = Map("MyKey" -> "MyVal"))
      assertCounters(3)
      assertResponse(response)
    }
  }

  test("firehose missing id/value") {
    val req = generateRequest("/api/v1/firehose", makeFirehosePayload(missingHeader = true))
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assert(StatusCodes.OK == status)
      assertParse()
      assertCounters(3)
      assertResponse(response, missing = true)
    }
  }

  test("firehose extra fields") {
    val req = generateRequest("/api/v1/firehose", makeFirehosePayload(extra = true))
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assert(StatusCodes.OK == status)
      assertParse()
      assertCounters(3, unknown = 1)
      assertResponse(response)
    }
  }

  test("firehose unexpected data field") {
    val req = generateRequest("/api/v1/firehose", makeFirehosePayload(badData = true))
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assert(StatusCodes.OK == status)
      assertParse(badData = true)
      assertCounters(2, dataParse = 1)
      assertResponse(response)
    }
  }

  test("firehose malformed json") {
    val req = generateRequest("/api/v1/firehose", makeFirehosePayload(malformed = true))
    val handler = new CloudWatchFirehoseEndpoint(registry, processor)
    req ~> handler.routes ~> check {
      assertEquals(status, StatusCodes.BadRequest)
      verify(processor, never).processDatapoints(processorCaptor, anyLong)
      assertCounters(outerParse = 1)
      assertResponse(response, withException = true)
    }
  }

  test("decode ok") {
    val json = makeJson(
      CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0)),
      CWDP("MetricB", Array[Double](1.0, 1.0, 1.0, 1.0), 2)
    )
    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained = decodeMetricJson(parser, List.empty)
      assertEquals(obtained, parsedDatapointsA)
    }
  }

  test("decode malformed") {
    val json = makeJson(
      CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0)),
      CWDP("MetricB", Array[Double](1.0, 1.0, 1.0, 1.0), 2)
    )
    intercept[JsonEOFException] {
      Using.resource(Json.newJsonParser(json.substring(0, 256))) { parser =>
        decodeMetricJson(parser, List.empty)
      }
    }
  }

  test("decode extra top-level field") {
    var json = makeJson(CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0)))
    json = json.substring(0, json.length - 2) + ",\"ignored\":\"field\"}"

    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained = decodeMetricJson(parser, List.empty)
      assertEquals(obtained, List(parsedDatapointsA(0)))
    }
  }

  test("decode missing metric") {
    val json = makeJson(CWDP(null, Array[Double](42.0, 10.0, 32.0, 2.0)))
    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained = decodeMetricJson(parser, List.empty)
      assertEquals(
        obtained,
        List(
          parsedDatapointsA(0).copy(
            metricName = null
          )
        )
      )
    }
  }

  test("decode missing timestamp") {
    import scala.compat.java8.FunctionConverters.*
    val json = makeJson(CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0), wTimestamp = false))
    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained = decodeMetricJson(parser, List.empty)
      assertEquals(
        obtained,
        List(
          parsedDatapointsA(0).copy(
            datapoint = parsedDatapointsA(0).datapoint.copy(
              asJavaConsumer[Datapoint.Builder](_.timestamp(null))
            )
          )
        )
      )
    }
  }

  test("decode extra value fields") {
    val json = makeJson(CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0, 1.0)))
    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained = decodeMetricJson(parser, List.empty)
      assertEquals(obtained, List(parsedDatapointsA(0)))
    }
  }

  test("decode missing values") {
    import scala.compat.java8.FunctionConverters.*
    val json = makeJson(CWDP("MetricA", Array[Double](42.0, 10.0, 32.0)))
    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained = decodeMetricJson(parser, List.empty)
      assertEquals(
        obtained,
        List(
          parsedDatapointsA(0).copy(
            datapoint = parsedDatapointsA(0).datapoint.copy(
              asJavaConsumer[Datapoint.Builder](_.sampleCount(null))
            )
          )
        )
      )
    }
  }

  test("decode w common tags") {
    val json = makeJson(CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0)))
    Using.resource(Json.newJsonParser(json)) { parser =>
      val obtained =
        decodeMetricJson(parser, List(Dimension.builder().name("Key").value("Value").build()))
      assertEquals(
        obtained,
        List(
          parsedDatapointsA(0).copy(
            dimensions = parsedDatapointsA(0).dimensions :+
              Dimension.builder().name("Key").value("Value").build()
          )
        )
      )
    }
  }

  def assertCounters(
    parsed: Long = 0,
    unknown: Long = 0,
    dataParse: Long = 0,
    outerParse: Long = 0
  ): Unit = {
    assertEquals(registry.counter("atlas.cloudwatch.firehose.parse.dps").count(), parsed)
    assertEquals(registry.counter("atlas.cloudwatch.firehose.parse.unknown").count(), unknown)
    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.firehose.parse.exception",
          "id",
          "data",
          "ex",
          "JsonParseException"
        )
        .count(),
      dataParse
    )
    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.firehose.parse.exception",
          "id",
          "outer",
          "ex",
          "CharConversionException"
        )
        .count(),
      outerParse
    )
  }

  def assertParse(badData: Boolean = false, attributes: Map[String, String] = Map.empty): Unit = {
    val expectedCount = if (badData) 1 else 2
    verify(processor, times(expectedCount)).processDatapoints(processorCaptor, anyLong)

    var expected = if (!attributes.isEmpty) {
      parsedDatapointsA.map { dp =>
        dp.copy(dimensions = dp.dimensions ++ attributes.map { t =>
          Dimension.builder().name(t._1).value(t._2).build()
        })
      }
    } else parsedDatapointsA

    assertEquals(processorCaptor.values(0), expected)
    if (!badData) {
      expected = if (!attributes.isEmpty) {
        parsedDatapointsB.map { dp =>
          dp.copy(dimensions = dp.dimensions ++ attributes.map { t =>
            Dimension.builder().name(t._1).value(t._2).build()
          })
        }
      } else parsedDatapointsB
      assertEquals(processorCaptor.values(1), expected)
    }
  }

  def assertResponse(
    response: HttpResponse,
    missing: Boolean = false,
    withException: Boolean = false
  ): Unit = {
    val bs = Await.result(response.entity.dataBytes.runReduce(_ ++ _), 1.seconds)
    val decoded = Json.decode[RequestId](bs.toArray)
    if (missing || withException) {
      assertNull(decoded.requestId)
      assertEquals(decoded.timestamp, 0L)
    } else {
      assertEquals(decoded.requestId, "0001")
      assertEquals(decoded.timestamp, timestamp + 35_000)
    }
    if (withException) {
      val string = new String(bs.toArray)
      assert(string.contains("Invalid"))
    }
  }
}

object CloudWatchFirehoseEndpointSuite {

  def makeJson(vals: CWDP*): String = {
    val stream = new ByteArrayOutputStream()
    Using.resource(Json.newJsonGenerator(stream)) { json =>
      vals.foreach { dp =>
        dp.encode(json)
        json.writeRaw("\n")
      }
    }
    new String(stream.toByteArray)
  }

  var parsedDatapointsA = List(
    FirehoseMetric(
      "Stream1",
      "UT/Test",
      "MetricA",
      List(
        Dimension.builder().name("AwsTag").value("AwsVal").build(),
        Dimension.builder().name("nf.account").value("1234").build(),
        Dimension.builder().name("nf.region").value("us-west-2").build()
      ),
      Datapoint
        .builder()
        .timestamp(Instant.ofEpochMilli(timestamp))
        .sum(42.0)
        .minimum(10.0)
        .maximum(32.0)
        .sampleCount(2.0)
        .unit("None")
        .build()
    ),
    FirehoseMetric(
      "Stream1",
      "UT/Test",
      "MetricB",
      List(
        Dimension.builder().name("nf.account").value("1234").build(),
        Dimension.builder().name("nf.region").value("us-west-2").build()
      ),
      Datapoint
        .builder()
        .timestamp(Instant.ofEpochMilli(timestamp))
        .sum(1.0)
        .minimum(1.0)
        .maximum(1.0)
        .sampleCount(1.0)
        .unit("None")
        .build()
    )
  )

  var parsedDatapointsB = List(
    FirehoseMetric(
      "Stream1",
      "UT/Test",
      "MetricC",
      List(
        Dimension.builder().name("AwsTag").value("AwsVal").build(),
        Dimension.builder().name("nf.account").value("1234").build(),
        Dimension.builder().name("nf.region").value("us-west-2").build()
      ),
      Datapoint
        .builder()
        .timestamp(Instant.ofEpochMilli(timestamp))
        .sum(2.0)
        .minimum(2.0)
        .maximum(2.0)
        .sampleCount(1.0)
        .unit("None")
        .build()
    )
  )

  def makeFirehosePayload(
    missingHeader: Boolean = false,
    empty: Boolean = false,
    extra: Boolean = false,
    badData: Boolean = false,
    malformed: Boolean = false
  ): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    Using.resource(Json.newJsonGenerator(stream)) { json =>
      json.writeStartObject()
      if (!missingHeader) {
        json.writeStringField("requestId", "0001")
        json.writeNumberField("timestamp", timestamp + 35_000)
      }
      json.writeArrayFieldStart("records")

      if (!empty) {
        json.writeStartObject()
        var data = makeJson(
          CWDP("MetricA", Array[Double](42.0, 10.0, 32.0, 2.0)),
          CWDP("MetricB", Array[Double](1.0, 1.0, 1.0, 1.0), 2)
        )
        json.writeStringField("data", Base64.getEncoder.encodeToString(data.getBytes()))
        json.writeEndObject()

        json.writeStartObject()
        if (badData) {
          data = "definitely not json"
        } else {
          data = makeJson(
            CWDP("MetricC", Array[Double](2.0, 2.0, 2.0, 1.0))
          )
        }
        json.writeStringField("data", Base64.getEncoder.encodeToString(data.getBytes()))
        json.writeEndObject()
      }

      json.writeEndArray()

      if (extra) json.writeStringField("Ignore", "me")
      json.writeEndObject()
    }
    val ba = stream.toByteArray
    if (malformed) {
      ba(0) = 0
      ba(1) = 0
      ba(2) = 0
      ba(3) = 0
    }
    ba
  }

  def generateRequest(
    uri: String,
    payload: Array[Byte] = makeFirehosePayload(),
    gzip: Boolean = false,
    attributes: Map[String, String] = null
  ): HttpRequest = {

    val bytes = if (gzip) {
      val baos = new ByteArrayOutputStream()
      val stream = new FastGzipOutputStream(baos)
      stream.write(payload)
      stream.close()
      baos.toByteArray
    } else payload

    HttpRequest(
      POST,
      uri,
      if (attributes != null) {
        Seq(
          RawHeader(
            "X-Amz-Firehose-Common-Attributes",
            Json.encode(Map("commonAttributes" -> attributes))
          )
        )
      } else {
        Seq.empty
      },
      entity = HttpEntity(ContentTypes.`application/json`, bytes)
    )
  }
}
