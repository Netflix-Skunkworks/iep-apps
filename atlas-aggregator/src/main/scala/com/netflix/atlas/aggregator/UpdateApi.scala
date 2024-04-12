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
package com.netflix.atlas.aggregator

import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.Spectator
import com.typesafe.scalalogging.StrictLogging

class UpdateApi(aggrService: AtlasAggregatorService) extends WebApi with StrictLogging {

  import UpdateApi.*

  require(aggrService != null, "no binding for aggregate registry")

  def routes: Route = {
    endpointPath("api" / "v4" / "update") {
      post {
        parseEntity(customJson(p => processPayload(p, aggrService))) { response =>
          complete(response)
        }
      }
    }
  }
}

object UpdateApi {

  private val registry = Spectator.globalRegistry()
  private val success = counter("success")
  private val dropped = counter("dropped")
  private val invalid = counter("invalid")

  private def counter(id: String): Counter = {
    registry.counter("atlas.aggr.datapoints", "id", id)
  }

  private val decoder = PayloadDecoder.default

  private[aggregator] def processPayload(
    parser: JsonParser,
    service: AtlasAggregatorService
  ): HttpResponse = {
    createResponse(decoder.decode(parser, service))
  }

  private val okResponse = {
    val entity = HttpEntity(MediaTypes.`application/json`, "{}")
    HttpResponse(StatusCodes.OK, entity = entity)
  }

  private def createErrorResponse(status: StatusCode, msg: FailureMessage): HttpResponse = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    HttpResponse(status, entity = entity)
  }

  private def createResponse(result: PayloadDecoder.Result): HttpResponse = {
    val numFailures = result.failures.size
    success.increment(result.numDatapoints - result.numDropped - numFailures)
    dropped.increment(result.numDropped)
    invalid.increment(numFailures)
    if (result.failures.isEmpty) {
      okResponse
    } else {
      if (result.numDatapoints > numFailures) {
        // Partial failure
        val msg = FailureMessage.partial(result.failures, numFailures)
        createErrorResponse(StatusCodes.Accepted, msg)
      } else {
        // All datapoints dropped
        val msg = FailureMessage.error(result.failures, numFailures)
        createErrorResponse(StatusCodes.BadRequest, msg)
      }
    }
  }
}
