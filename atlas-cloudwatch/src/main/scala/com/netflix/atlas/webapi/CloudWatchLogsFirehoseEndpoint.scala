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
package com.netflix.atlas.webapi

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.MalformedRequestContentRejection
import org.apache.pekko.http.scaladsl.server.Route
import tools.jackson.core.JsonParser
import com.netflix.atlas.pekko.CustomDirectives.customJson
import com.netflix.atlas.pekko.CustomDirectives.endpointPath
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.json3.Json
import com.netflix.atlas.json3.JsonParserHelper.foreachField
import com.netflix.atlas.json3.JsonParserHelper.foreachItem
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.Base64
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import scala.util.Using

class CloudWatchLogsFirehoseEndpoint(
  registry: Registry,
  logsProcessor: CloudWatchLogsProcessor
)(implicit val system: ActorSystem)
    extends WebApi
    with StrictLogging {

  private val recordsReceived =
    registry.counter("atlas.cloudwatchlogs.firehose.records")

  private val logEventsReceived =
    registry.counter("atlas.cloudwatchlogs.firehose.logEvents")

  private val unknownField =
    registry.counter("atlas.cloudwatchlogs.firehose.parse.unknown")

  private val parseException =
    registry.createId("atlas.cloudwatchlogs.firehose.parse.exception")

  // Parsing, gunzip, and rule-matching for a batch of log records runs here, isolated
  // from the default Pekko dispatcher, so a burst on the metrics Firehose path (which
  // has its own dedicated dispatcher) or other default-dispatcher work can't starve or
  // be starved by logs ingestion.
  private val firehoseDispatcher: ExecutionContext =
    system.dispatchers.lookup("logs-firehose-dispatcher")

  override def routes: Route = {
    (post | put) {
      endpointPath("api" / "v1" / "firehoseLogs") {
        handleReq
      }
    }
  }

  private def handleReq: Route = {
    extractRequestContext { ctx =>
      val decode = customJson(p => parseFirehoseMessage(p))
      val mediaType = ctx.request.entity.contentType.mediaType
      val bytesFuture = ctx.request.entity.dataBytes.runReduce(_ ++ _)(ctx.materializer)
      val responseFuture = bytesFuture.map(decode(mediaType))(firehoseDispatcher)
      onComplete(responseFuture) {
        case Success(response) =>
          if (response.exception.isDefined) {
            complete(StatusCodes.BadRequest, response.getEntity)
          } else {
            complete(StatusCodes.OK, response.getEntity)
          }
        case Failure(t) =>
          reject(MalformedRequestContentRejection("invalid request payload", t))
      }
    }
  }

  private def parseFirehoseMessage(parser: JsonParser): RequestId = {
    var requestId: String = null
    var timestamp: Long = 0L

    try {
      foreachField(parser) {
        case "requestId" =>
          requestId = parser.nextStringValue()
        case "timestamp" =>
          timestamp = parser.nextLongValue(0L)
        case "records" =>
          foreachItem(parser) {
            foreachField(parser) {
              case "data" =>
                val b64 = parser.nextStringValue()
                if (b64 != null) {
                  try {
                    val bytes = Base64.getDecoder.decode(b64)
                    Using.resource(inputStream(bytes)) { in =>
                      Using.resource(Json.newJsonParser(in)) { p =>
                        val count = decodeAndProcessCloudWatchLogsPayload(p)
                        recordsReceived.increment()
                        logEventsReceived.increment(count)
                      }
                    }
                  } catch {
                    case ex: Exception =>
                      logger.warn(
                        "Unexpected data parse exception for CW Logs firehose record",
                        ex
                      )
                      incrementException(ex, data = true)
                  }
                } else {
                  logger.warn("Skipping firehose record with null data field")
                }

              case unknown =>
                logger.warn("Skipping unknown firehose record field: {}", unknown)
                parser.nextToken()
                parser.skipChildren()
                unknownField.increment()
            }
          }

        case unknown =>
          logger.warn("Skipping unknown firehose field: {}", unknown)
          parser.nextToken()
          parser.skipChildren()
          unknownField.increment()
      }

      RequestId(requestId, timestamp)
    } catch {
      case ex: Exception =>
        logger.error("Failed to parse CW Logs firehose request", ex)
        incrementException(ex)
        RequestId(requestId, timestamp, Some(ex))
    }
  }

  private def isGzip(bytes: Array[Byte]): Boolean =
    bytes.length > 2 &&
    bytes(0) == 0x1F.toByte && bytes(1) == 0x8B.toByte

  /**
   * Create an InputStream for reading the content of the bytes. If the data is
   * gzip compressed, then it will be wrapped in a GZIPInputStream to handle the
   * decompression of the data.
   */
  private def inputStream(bytes: Array[Byte]): InputStream = {
    val in: InputStream = new ByteArrayInputStream(bytes)
    if (isGzip(bytes)) new GZIPInputStream(in) else in
  }

  /**
   * Decode a single CloudWatch Logs payload (decoded from Firehose record.data),
   * extract subscriptionFilters and logEvents, and send them to the logsProcessor.
   *
   * Returns number of logEvents processed.
   */
  private def decodeAndProcessCloudWatchLogsPayload(parser: JsonParser): Int = {

    var messageType: String = null
    var logGroup: String = null
    var logStream: String = null
    var owner: String = null
    val subscriptionFilters = ListBuffer.empty[String]
    val events = ListBuffer.empty[CloudWatchLogEvent]

    foreachField(parser) {
      case "messageType" =>
        messageType = parser.nextStringValue()

      case "owner" =>
        owner = parser.nextStringValue()

      case "logGroup" =>
        logGroup = parser.nextStringValue()

      case "logStream" =>
        logStream = parser.nextStringValue()

      case "subscriptionFilters" =>
        foreachItem(parser) {
          val value = parser.getValueAsString()
          if (value != null) {
            subscriptionFilters += value
          }
        }

      case "logEvents" =>
        foreachItem(parser) {
          events += decodeSingleLogEvent(parser)
        }

      case other =>
        logger.warn(s"Skipping unknown firehose logs payload field: $other")
        parser.nextToken()
        parser.skipChildren()
    }

    if (messageType != "DATA_MESSAGE") {
      logger.debug(
        s"Skipping CW Logs payload with messageType=$messageType, " +
          s"logGroup=$logGroup, logStream=$logStream, owner=$owner"
      )
      return 0
    }

    val filters = subscriptionFilters.toList

    if (events.nonEmpty) {
      logsProcessor.process(
        owner = owner,
        logGroup = logGroup,
        logStream = logStream,
        subscriptionFilters = filters,
        events = events.toList
      )
    }

    events.size
  }

  private def decodeSingleLogEvent(parser: JsonParser): CloudWatchLogEvent = {
    var id: String = null
    var timestamp: Long = 0L
    var message: String = null
    var account: String = null
    var region: String = null
    val extractedFields = Map.newBuilder[String, String]

    foreachField(parser) {
      case "id" =>
        id = parser.nextStringValue()
      case "timestamp" =>
        timestamp = parser.nextLongValue(0L)
      case "message" =>
        message = parser.nextStringValue()
      case "extractedFields" =>
        foreachField(parser) {
          case "@aws.account" =>
            account = parser.nextStringValue()
          case "@aws.region" =>
            region = parser.nextStringValue()
          case field =>
            val value = parser.nextStringValue()
            if (value != null) {
              extractedFields += field -> value
            }
        }
      case _ =>
        parser.nextToken()
        parser.skipChildren()
    }

    CloudWatchLogEvent(
      id = id,
      timestamp = timestamp,
      message = message,
      account = Option(account),
      region = Option(region),
      extractedFields = extractedFields.result()
    )
  }

  private def incrementException(ex: Exception, data: Boolean = false): Unit = {
    val id = if (data) "data" else "outer"
    registry
      .counter(parseException.withTags("id", id, "ex", ex.getClass.getSimpleName))
      .increment()
  }
}

/** One CW log event from logEvents[]. */
final case class CloudWatchLogEvent(
  id: String,
  timestamp: Long,
  message: String,
  account: Option[String],
  region: Option[String],
  extractedFields: Map[String, String] = Map.empty
)

/**
 * Processor interface now includes subscriptionFilters.
 */
trait CloudWatchLogsProcessor {

  def process(
    owner: String,
    logGroup: String,
    logStream: String,
    subscriptionFilters: List[String],
    events: List[CloudWatchLogEvent]
  ): Unit
}
