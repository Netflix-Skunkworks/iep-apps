package com.netflix.atlas.webapi

import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods.POST
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.atlas.json3.Json
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import junit.framework.TestCase.assertNull
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.*

import java.io.ByteArrayOutputStream
import java.util.Base64
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Using

class CloudWatchLogsFirehoseEndpointSuite extends MUnitRouteSuite with StrictLogging {

  var registry: Registry = _
  var logsProcessor: CloudWatchLogsProcessor = _
  var processorCaptor: ArgumentCaptor[List[CloudWatchLogEvent]] = _

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    logsProcessor = mock(classOf[CloudWatchLogsProcessor])
    processorCaptor = ArgumentCaptor.forClass(classOf[List[CloudWatchLogEvent]])
  }

  test("logs firehose success") {
    val payload = CloudWatchLogsFirehoseEndpointSuite.makeFirehosePayload()
    val req = CloudWatchLogsFirehoseEndpointSuite.generateRequest("/api/v2/firehose", payload)

    val handler = new CloudWatchLogsFirehoseEndpoint(registry, logsProcessor)
    req ~> handler.routes ~> check {
      assertEquals(StatusCodes.OK, status)
      System.out.print("v2/status : " + status)
      assertParse(expectedEventsTotal = 4)
      assertCounters(parsedRecords = 2, parsedEvents = 4)
      assertResponse(response, missing = false, withException = false)
    }
  }

  test("logs firehose non DATA_MESSAGE is skipped") {
    val payload =
      CloudWatchLogsFirehoseEndpointSuite.makeFirehosePayload(nonDataMessage = true)
    val req = CloudWatchLogsFirehoseEndpointSuite.generateRequest("/api/v2/firehose", payload)

    val handler = new CloudWatchLogsFirehoseEndpoint(registry, logsProcessor)
    req ~> handler.routes ~> check {
      assertEquals(StatusCodes.OK, status)
      // No events processed
      verify(logsProcessor, never()).process(any(), any(), any(), any(), any())
      assertCounters(parsedRecords = 1)
      assertResponse(response, missing = false, withException = false)
    }
  }

  test("logs firehose malformed outer json") {
    val payload =
      CloudWatchLogsFirehoseEndpointSuite.makeFirehosePayload(malformed = true)
    val req = CloudWatchLogsFirehoseEndpointSuite.generateRequest("/api/v2/firehose", payload)

    val handler = new CloudWatchLogsFirehoseEndpoint(registry, logsProcessor)
    req ~> handler.routes ~> check {
      assertEquals(StatusCodes.BadRequest, status)
      verify(logsProcessor, never()).process(any(), any(), any(), any(), any())
      assertCounters(outerParse = 1)
      assertResponse(response, missing = true, withException = true)
    }
  }

  private def assertCounters(
    parsedRecords: Long = 0,
    parsedEvents: Long = 0,
    unknown: Long = 0,
    dataParse: Long = 0,
    outerParse: Long = 0
  ): Unit = {
    assertEquals(
      parsedRecords,
      registry.counter("atlas.cloudwatchlogs.firehose.records").count()
    )
    assertEquals(
      parsedEvents,
      registry.counter("atlas.cloudwatchlogs.firehose.logEvents").count()
    )
    assertEquals(
      unknown,
      registry.counter("atlas.cloudwatchlogs.firehose.parse.unknown").count()
    )
    assertEquals(
      dataParse,
      registry
        .counter(
          "atlas.cloudwatchlogs.firehose.parse.exception",
          "id",
          "data",
          "ex",
          "StreamReadException"
        )
        .count()
    )
    assertEquals(
      outerParse,
      registry
        .counter(
          "atlas.cloudwatchlogs.firehose.parse.exception",
          "id",
          "outer",
          "ex",
          "JacksonIOException"
        )
        .count()
    )
  }

  private def assertParse(expectedEventsTotal: Int): Unit = {
    // logsProcessor.process(owner, logGroup, logStream, filters, events)
    verify(logsProcessor, atLeastOnce())
      .process(
        any(),
        any(),
        any(),
        any(),
        processorCaptor.capture()
      )

    val allBatches = processorCaptor.getAllValues
    val totalEvents = allBatches.toArray
      .map(_.asInstanceOf[List[CloudWatchLogEvent]].size)
      .sum

    assertEquals(expectedEventsTotal, totalEvents)
  }

  private def assertResponse(
    response: HttpResponse,
    missing: Boolean,
    withException: Boolean
  ): Unit = {
    val bs = Await.result(response.entity.dataBytes.runReduce(_ ++ _), 1.seconds)
    val decoded = Json.decode[RequestId](bs.toArray)

    if (missing || withException) {
      assertNull(decoded.requestId)
      assertEquals(0L, decoded.timestamp)
    } else {
      assertEquals("0001", decoded.requestId)
      assert(decoded.timestamp > 0L)
    }
  }
}

object CloudWatchLogsFirehoseEndpointSuite {

  /**
   * Create one CW Logs payload JSON as it would appear inside record.data:
   *
   * {
   *   "messageType": "DATA_MESSAGE",
   *   "owner": "123456789012",
   *   "logGroup": "/aws/lambda/my-func",
   *   "logStream": "2026/04/17/[$LATEST]abc",
   *   "subscriptionFilters": ["my-sub"],
   *   "logEvents": [
   *     { "id": "id1", "timestamp": 1, "message": "m1",
   *       "extractedFields": {"@aws.account": "1234", "@aws.region": "us-east-1"} },
   *     { "id": "id2", "timestamp": 2, "message": "m2" }
   *   ]
   * }
   */
  private def makeSingleLogsJson(
    mtype: String
  ): String = {
    val baos = new ByteArrayOutputStream()
    Using.resource(Json.newJsonGenerator(baos)) { json =>
      json.writeStartObject()
      json.writeStringProperty("messageType", mtype)
      json.writeStringProperty("owner", "123456789012")
      json.writeStringProperty("logGroup", "/aws/lambda/my-func")
      json.writeStringProperty("logStream", "2026/04/17/[$LATEST]abc")
      json.writeArrayPropertyStart("subscriptionFilters")
      json.writeString("my-sub")
      json.writeEndArray()
      json.writeArrayPropertyStart("logEvents")

      // event 1
      json.writeStartObject()
      json.writeStringProperty("id", "id1")
      json.writeNumberProperty("timestamp", 1L)
      json.writeStringProperty("message", "m1")
      json.writeObjectPropertyStart("extractedFields")
      json.writeStringProperty("@aws.account", "1234")
      json.writeStringProperty("@aws.region", "us-east-1")
      json.writeEndObject() // extractedFields
      json.writeEndObject() // event 1

      // event 2
      json.writeStartObject()
      json.writeStringProperty("id", "id2")
      json.writeNumberProperty("timestamp", 2L)
      json.writeStringProperty("message", "m2")
      json.writeEndObject() // event 2

      json.writeEndArray() // logEvents
      json.writeEndObject()
    }
    new String(baos.toByteArray)
  }

  /**
   * Outer firehose payload with requestId/timestamp and records[].data (base64 logs JSON).
   */
  def makeFirehosePayload(
    empty: Boolean = false,
    nonDataMessage: Boolean = false,
    malformed: Boolean = false
  ): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    Using.resource(Json.newJsonGenerator(baos)) { json =>
      json.writeStartObject()
      json.writeStringProperty("requestId", "0001")
      json.writeNumberProperty("timestamp", System.currentTimeMillis())
      json.writeArrayPropertyStart("records")

      if (!empty) {
        // First record: DATA_MESSAGE or non-DATA_MESSAGE
        json.writeStartObject()
        val logsJson1 = makeSingleLogsJson(
          mtype = if (nonDataMessage) "CONTROL_MESSAGE" else "DATA_MESSAGE"
        )
        println(s"logsJson1 = $logsJson1")
        json.writeStringProperty(
          "data",
          Base64.getEncoder.encodeToString(logsJson1.getBytes("UTF-8"))
        )
        json.writeEndObject()

        // Second record: always DATA_MESSAGE
        if (!nonDataMessage) {
          json.writeStartObject()
          val logsJson2 = makeSingleLogsJson("DATA_MESSAGE")
          json.writeStringProperty(
            "data",
            Base64.getEncoder.encodeToString(logsJson2.getBytes("UTF-8"))
          )
          json.writeEndObject()
        }
      }

      json.writeEndArray()
      json.writeEndObject()
    }
    val ba = baos.toByteArray
    if (malformed && ba.length >= 4) {
      ba(0) = 0
      ba(1) = 0
      ba(2) = 0
      ba(3) = 0
    }
    ba
  }

  def generateRequest(
    uri: String,
    payload: Array[Byte]
  ): HttpRequest = {
    HttpRequest(
      method = POST,
      uri = uri,
      entity = HttpEntity(ContentTypes.`application/json`, payload)
    )
  }
}
