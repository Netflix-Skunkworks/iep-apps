/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.iep.lwc

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonParserHelper._
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.histogram.BucketFunctions
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

class BridgeApi(
  config: Config,
  registry: Registry,
  evaluator: ExpressionsEvaluator,
  implicit val system: ActorSystem
) extends WebApi
    with StrictLogging {

  import com.netflix.atlas.akka.OpportunisticEC._

  override def routes: Route = {
    post {
      endpointPath("api" / "v1" / "publish") {
        handleReq
      } ~
      endpointPath("api" / "v1" / "publish-fast") {
        // Legacy path from when there was more than one publish mode
        handleReq
      }
    }
  }

  private val evalUri = Uri(config.getString("netflix.iep.lwc.bridge.eval-uri"))

  private val step = DefaultSettings.stepSize

  // Number of invalid datapoints received
  private val numReceivedCounter = BucketCounter.get(
    registry,
    registry.createId("atlas.db.numMetricsReceived"),
    BucketFunctions.ageBiasOld(step, TimeUnit.MILLISECONDS)
  )

  private def handleReq: Route = {
    parseEntity(customJson(p => BridgeApi.decodeBatch(p))) { values =>
      process(values)
      complete(HttpResponse(StatusCodes.OK))
    }
  }

  /**
    * Evaluate the datapoints against the full set of expressions and forward the results
    * to the LWC service.
    */
  private def process(values: List[BridgeDatapoint]): Unit = {
    val now = registry.clock().wallTime()
    values.foreach { v =>
      numReceivedCounter.record(now - v.timestamp)
    }

    if (values.nonEmpty) {
      val timestamp = fixTimestamp(values.head.timestamp)
      val payload = evaluator.eval(timestamp, values)
      if (!payload.getMetrics.isEmpty || !payload.getMessages.isEmpty) {
        val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(payload))
        val request = HttpRequest(HttpMethods.POST, evalUri, Nil, entity)
        mkRequest("lwc-eval", request).foreach { response =>
          response.discardEntityBytes()
        }
      }
    }
  }

  private def mkRequest(name: String, request: HttpRequest): Future[HttpResponse] = {
    val accessLogger = AccessLogger.newClientLogger(name, request)
    Http()(system).singleRequest(request).andThen { case t => accessLogger.complete(t) }
  }

  private def fixTimestamp(t: Long): Long = t / step * step
}

object BridgeApi {

  // Default value based on current validation settings
  private val maxPermittedTags = 40

  def decodeBatch(json: String): List[BridgeDatapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeBatch(parser)
    finally parser.close()
  }

  def decodeBatch(parser: JsonParser): List[BridgeDatapoint] = {
    val commonTags = new Array[String](maxPermittedTags * 2)
    var i = 0

    var commonName: String = null
    var metrics: List[BridgeDatapoint] = null

    foreachField(parser) {
      case "tags" =>
        foreachField(parser) {
          case "name" =>
            // Shouldn't be present on common tags
            commonName = parser.nextTextValue()
          case key =>
            val value = parser.nextTextValue()
            if (value != null) {
              commonTags(i) = key
              commonTags(i + 1) = value
              i += 2
            }
        }
      case "metrics" =>
        val builder = List.newBuilder[BridgeDatapoint]
        foreachItem(parser) {
          val tags = new Array[String](maxPermittedTags * 2)
          var j = 0
          var name: String = null
          var timestamp: Long = -1L
          var value: Double = Double.NaN
          foreachField(parser) {
            case "tags" =>
              foreachField(parser) {
                case "name" =>
                  name = parser.nextTextValue()
                case key =>
                  val value = parser.nextTextValue()
                  if (value != null) {
                    tags(j) = key
                    tags(j + 1) = value
                    j += 2
                  }
              }
            case "timestamp" =>
              timestamp = nextLong(parser)
            case "value" =>
              value = nextDouble(parser)
            case "start" =>
              timestamp = nextLong(parser) // Legacy support
            case "values" =>
              value = getValue(parser)
            case _ => // Ignore unknown fields
              parser.nextToken()
              parser.skipChildren()
          }
          builder += new BridgeDatapoint(name, tags, j, timestamp, value)
        }
        metrics = builder.result()
    }

    // Add in common tags
    if (metrics == null) {
      Nil
    } else {
      metrics.filter { m =>
        m.merge(commonName, commonTags, i)
        m.name != null
      }
    }
  }

  private def getValue(parser: JsonParser): Double = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.nextToken() match {
      case START_ARRAY        => nextDouble(parser)
      case VALUE_NUMBER_FLOAT => parser.getValueAsDouble()
      case VALUE_STRING       => java.lang.Double.valueOf(parser.getText())
      case t                  => fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
    }
  }
}
