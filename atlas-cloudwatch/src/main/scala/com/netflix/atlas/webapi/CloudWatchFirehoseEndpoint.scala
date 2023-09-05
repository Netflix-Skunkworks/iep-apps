/*
 * Copyright 2014-2023 Netflix, Inc.
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
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.pekko.CustomDirectives.customJson
import com.netflix.atlas.pekko.CustomDirectives.endpointPath
import com.netflix.atlas.pekko.CustomDirectives.parseEntity
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor
import com.netflix.atlas.cloudwatch.FirehoseMetric
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonParserHelper.foreachField
import com.netflix.atlas.json.JsonParserHelper.foreachItem
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpoint.decodeCommonTags
import com.netflix.atlas.webapi.CloudWatchFirehoseEndpoint.decodeMetricJson
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import java.time.Instant
import java.util.Base64
import scala.collection.mutable
import scala.util.Using

class CloudWatchFirehoseEndpoint(
  registry: Registry,
  processor: CloudWatchMetricsProcessor
)(implicit val system: ActorSystem)
    extends WebApi
    with StrictLogging {

  private val dataReceived = registry.counter("atlas.cloudwatch.firehose.parse.dps")
  private val unknownField = registry.counter("atlas.cloudwatch.firehose.parse.unknown")
  private val parseException = registry.createId("atlas.cloudwatch.firehose.parse.exception")

  override def routes: Route = {
    (post | put) {
      endpointPath("api" / "v1" / "firehose") {
        handleReq
      } ~
      complete(StatusCodes.NotFound)
    }
  }

  private def handleReq: Route = {
    extractRequestContext { ctx =>
      val commonTags = decodeCommonTags(ctx.request)
      parseEntity(customJson(p => parseFirehoseMessage(p, commonTags))) { response =>
        if (response.exception.isDefined) {
          complete(StatusCodes.BadRequest, response.getEntity)
        } else {
          complete(StatusCodes.OK, response.getEntity)
        }
      }
    }
  }

  private def parseFirehoseMessage(parser: JsonParser, commonTags: List[Dimension]): RequestId = {
    var requestId: String = null
    var timestamp: Long = 0
    val receivedTimestamp = System.currentTimeMillis()
    try {
      foreachField(parser) {
        case "requestId" => requestId = parser.nextTextValue()
        case "timestamp" => timestamp = parser.nextLongValue(0L)
        case "records" =>
          foreachItem(parser) {
            foreachField(parser) {
              case "data" =>
                try {
                  // catch the exception here in the case one data set is errant but the others are alright.
                  // At least we should get _some_ data that way.
                  Using.resource(
                    Json.newJsonParser(Base64.getDecoder.decode(parser.nextTextValue()))
                  ) { p =>
                    val datapoints = decodeMetricJson(p, commonTags)
                    processor.processDatapoints(datapoints, receivedTimestamp)
                    dataReceived.increment(datapoints.size)
                  }
                } catch {
                  case ex: Exception =>
                    logger.warn("Unexpected data parse exception", ex)
                    incrementException(ex, true)
                }
              case unknown => // Ignore unknown fields
                logger.debug("Skipping unknown firehose record field: {}", unknown)
                parser.nextToken()
                parser.skipChildren()
                unknownField.increment()
            }
          }
        case unknown => // Ignore unknown fields
          logger.debug("Skipping unknown firehose field: {}", unknown)
          parser.nextToken()
          parser.skipChildren()
          unknownField.increment()
      }
      RequestId(requestId, timestamp)
    } catch {
      case ex: Exception =>
        logger.error("Failed to parse request", ex)
        incrementException(ex)
        RequestId(requestId, timestamp, Some(ex))
    }
  }

  private def incrementException(ex: Exception, data: Boolean = false): Unit = {
    val id = if (data) "data" else "outer"
    registry.counter(parseException.withTags("id", id, "ex", ex.getClass.getSimpleName)).increment()
  }
}

object CloudWatchFirehoseEndpoint extends StrictLogging {

  private[webapi] def decodeMetricJson(
    parser: JsonParser,
    commonTags: List[Dimension]
  ): List[FirehoseMetric] = {
    // JSON stream format, i.e. new line separated list of objects.
    var t = parser.nextToken()
    val results = List.newBuilder[FirehoseMetric]

    var dp = Datapoint.builder()
    var metricStreamName: String = null
    var accountId: String = null
    var region: String = null
    var namespace: String = null
    var metricName: String = null
    var dimensions = List.newBuilder[Dimension]
    while (t != null) {
      foreachField(parser) {
        case "metric_stream_name" => metricStreamName = parser.nextTextValue()
        case "account_id"         => accountId = parser.nextTextValue
        case "region"             => region = parser.nextTextValue
        case "namespace"          => namespace = parser.nextTextValue
        case "metric_name"        => metricName = parser.nextTextValue
        case "unit"               => dp.unit(parser.nextTextValue())
        case "dimensions"         => decodeDimensions(parser, dimensions)
        case "timestamp" =>
          dp.timestamp(Instant.ofEpochMilli(parser.nextLongValue(0L)))
        case "value" => decodeValue(parser, dp)
        case unknown => // Ignore unknown fields
          logger.debug("Skipping unknown firehose metric field: {}", unknown)
          parser.nextToken()
          parser.skipChildren()
      }

      if (accountId != null) {
        dimensions += Dimension
          .builder()
          .name("nf.account")
          .value(accountId)
          .build()
      }
      if (region != null) {
        dimensions += Dimension
          .builder()
          .name("nf.region")
          .value(region)
          .build()
      }
      dimensions ++= commonTags

      results += FirehoseMetric(
        metricStreamName,
        namespace,
        metricName,
        dimensions.result(),
        dp.build()
      )

      // reset
      dp = Datapoint.builder()
      metricStreamName = null
      accountId = null
      region = null
      namespace = null
      metricName = null
      dimensions = List.newBuilder[Dimension]

      // advance
      t = parser.nextToken()
    }
    results.result()
  }

  private[webapi] def decodeDimensions(
    parser: JsonParser,
    dimensions: mutable.Builder[Dimension, List[Dimension]]
  ): Unit = {
    foreachField(parser) {
      case key =>
        val value = parser.nextTextValue()
        if (value != null) {
          dimensions += Dimension
            .builder()
            .name(key)
            .value(value)
            .build()
        }
    }
  }

  private[webapi] def decodeValue(parser: JsonParser, dp: Datapoint.Builder): Unit = {
    foreachField(parser) {
      case "sum" =>
        parser.nextToken()
        dp.sum(parser.getDoubleValue)
      case "count" =>
        parser.nextToken()
        dp.sampleCount(parser.getDoubleValue)
      case "max" =>
        parser.nextToken()
        dp.maximum(parser.getDoubleValue)
      case "min" =>
        parser.nextToken()
        dp.minimum(parser.getDoubleValue)
      case unknown => // Ignore unknown fields
        // TODO - Extend this if we need to record any of the other fields offered by AWS.
        // Note that each field is an additional financial cost though.
        logger.debug("Skipping unknown value field: {}", unknown)
        parser.nextToken()
        parser.skipChildren()
    }
  }

  private[webapi] def decodeCommonTags(request: HttpRequest): List[Dimension] = {
    val hdr = request.getHeader("X-Amz-Firehose-Common-Attributes")
    if (hdr.isPresent) {
      Using.resource(Json.newJsonParser(hdr.get().value())) { parser =>
        val dimensions = List.newBuilder[Dimension]
        foreachField(parser) {
          case "commonAttributes" => decodeDimensions(parser, dimensions)
        }
        dimensions.result()
      }
    } else {
      List.empty
    }
  }
}
