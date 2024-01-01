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
package com.netflix.iep.lwc.fwd.admin

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.dispatch.MessageDispatcher
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.entity
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller.*
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.Report
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

class Api(
  registry: Registry,
  schemaValidation: SchemaValidation,
  cwExprValidations: CwExprValidations,
  markerService: MarkerService,
  purger: Purger,
  exprDetailsDao: ExpressionDetailsDao,
  system: ActorSystem
) extends WebApi
    with StrictLogging {

  private implicit val configUnmarshaller: Unmarshaller[HttpEntity, JsonNode] =
    byteArrayUnmarshaller.map(Json.decode[JsonNode](_))

  private implicit val blockingDispatcher: MessageDispatcher =
    system.dispatchers.lookup("blocking-dispatcher")

  def routes: Route = {

    endpointPath("api" / "v1" / "cw" / "check", Remaining) { key =>
      post {
        entity(as[JsonNode]) { json =>
          complete {
            schemaValidation.validate(key, json)
            cwExprValidations.validate(key, json)

            HttpResponse(StatusCodes.OK)
          }
        }
      }
    } ~
    endpointPath("api" / "v1" / "cw" / "report") {
      post {
        entity(as[JsonNode]) { json =>
          complete {
            Json
              .decode[List[Report]](json)
              .foreach { report =>
                val enqueued = markerService.queue.offer(report)
                if (!enqueued) {
                  logger.warn(s"Unable to queue report $report")
                }
              }
            HttpResponse(StatusCodes.OK)
          }
        }
      }
    } ~
    endpointPath("api" / "v1" / "cw" / "expr" / "purgeEligible") {
      get {
        parameter("events".as(CsvSeq[String])) { events =>
          complete {
            Future {
              val body = Json.encode(
                exprDetailsDao.queryPurgeEligible(
                  System.currentTimeMillis(),
                  events.toList
                )
              )

              HttpResponse(
                StatusCodes.OK,
                entity = HttpEntity(MediaTypes.`application/json`, body)
              )
            }
          }
        }
      }
    } ~
    endpointPath("api" / "v1" / "cw" / "expr" / "purge") {
      delete {
        entity(as[JsonNode]) { json =>
          complete {
            purger.purge(Json.decode[List[ExpressionId]](json))
          }
        }
      }
    }
  }

}
