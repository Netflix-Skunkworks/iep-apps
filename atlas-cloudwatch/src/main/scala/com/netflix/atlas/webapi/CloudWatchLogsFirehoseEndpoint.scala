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
import org.apache.pekko.http.scaladsl.server.Route
import tools.jackson.core.JsonParser

import com.netflix.atlas.pekko.CustomDirectives.customJson
import com.netflix.atlas.pekko.CustomDirectives.endpointPath
import com.netflix.atlas.pekko.CustomDirectives.parseEntity
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.json3.Json
import com.netflix.atlas.json3.JsonParserHelper.{foreachField, foreachItem}
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import java.util.Base64
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

  override def routes: Route = {
    (post | put) {
      endpointPath("api" / "v2" / "firehose") {
        handleReq
      } ~
        complete(StatusCodes.NotFound)
    }
  }

  private def handleReq: Route = {
    extractRequestContext { _ =>
      parseEntity(customJson(p => parseFirehoseMessage(p))) { response =>
        if (response.exception.isDefined) {
          complete(StatusCodes.BadRequest, response.getEntity)
        } else {
          complete(StatusCodes.OK, response.getEntity)
        }
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
                    Using.resource(Json.newJsonParser(bytes)) { p =>
                      val count = decodeAndProcessCloudWatchLogsPayload(p)
                      recordsReceived.increment()
                      logEventsReceived.increment(count)
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

  /**
   * Decode a single CloudWatch Logs payload (decoded from Firehose record.data),
   * extract subscriptionFilters and logEvents, and send them to the logsProcessor.
   *
   * Returns number of logEvents processed.
   */
  private def decodeAndProcessCloudWatchLogsPayload(parser: JsonParser): Int = {
    import scala.collection.mutable.ListBuffer

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
          subscriptionFilters += parser.nextStringValue()
        }
      case "logEvents" =>
        foreachItem(parser) {
          events += decodeSingleLogEvent(parser)
        }
      case _ =>
        parser.nextToken()
        parser.skipChildren()
    }
    
    if (messageType != "DATA_MESSAGE") {
      logger.info(
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
          case _ =>
            parser.nextToken()
            parser.skipChildren()
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
      region = Option(region)
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
  region: Option[String]
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