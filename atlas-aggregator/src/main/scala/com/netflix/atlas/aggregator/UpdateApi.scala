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
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.eval.stream.Evaluator
import com.typesafe.scalalogging.StrictLogging

class UpdateApi(
  evaluator: Evaluator,
  aggrService: AtlasAggregatorService
) extends WebApi
    with StrictLogging {

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

  private val decoder = PayloadDecoder.default

  private[aggregator] def processPayload(
    parser: JsonParser,
    service: AtlasAggregatorService
  ): HttpResponse = {
    val result = decoder.decode(parser, service)
    createResponse(result.numDatapoints, result.failures)
  }

  private val okResponse = {
    val entity = HttpEntity(MediaTypes.`application/json`, "{}")
    HttpResponse(StatusCodes.OK, entity = entity)
  }

  private def createErrorResponse(status: StatusCode, msg: FailureMessage): HttpResponse = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    HttpResponse(status, entity = entity)
  }

  private def createResponse(numDatapoints: Int, failures: List[ValidationResult]): HttpResponse = {
    if (failures.isEmpty) {
      okResponse
    } else {
      val numFailures = failures.size
      if (numDatapoints > numFailures) {
        // Partial failure
        val msg = FailureMessage.partial(failures, numFailures)
        createErrorResponse(StatusCodes.Accepted, msg)
      } else {
        // All datapoints dropped
        val msg = FailureMessage.error(failures, numFailures)
        createErrorResponse(StatusCodes.BadRequest, msg)
      }
    }
  }
}
