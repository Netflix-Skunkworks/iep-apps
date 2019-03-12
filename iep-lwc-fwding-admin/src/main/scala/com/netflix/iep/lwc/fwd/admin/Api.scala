/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.entity
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.Report
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

class Api(
  registry: Registry,
  schemaValidation: SchemaValidation,
  cwExprValidations: CwExprValidations,
  markerService: MarkerService,
  exprDetailsDao: ExpressionDetailsDao,
  system: ActorSystem
) extends WebApi
    with StrictLogging {

  private implicit val configUnmarshaller =
    byteArrayUnmarshaller.map(Json.decode[JsonNode](_))

  implicit val blockingDispatcher = system.dispatchers.lookup("blocking-dispatcher")

  def routes: Route = {

    endpointPath("api" / "v1" / "check" / "cwf", Remaining) { key =>
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
    endpointPath("api" / "v1" / "report") {
      post {
        entity(as[JsonNode]) { json =>
          complete {
            Json
              .decode[List[Report]](json)
              .map { report =>
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
    endpointPath("api" / "v1" / "cw" / "report") {
      post {
        entity(as[JsonNode]) { json =>
          complete {
            Json
              .decode[List[Report]](json)
              .map { report =>
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
    }

  }

}
