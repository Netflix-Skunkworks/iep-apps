/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas.persistence

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.webapi.PublishApi

class PersistenceApi(localFileService: LocalFilePersistService) extends WebApi {

  override def routes: Route = {
    post {
      endpointPath("api" / "v1" / "persistence") {
        handleReq
      } ~ endpointPath("api" / "v1" / "publish") {
        handleReq
      } ~ endpointPath("api" / "v1" / "publish-fast") {
        // Legacy path from when there was more than one publish mode
        handleReq
      }
    }
  }

  private def handleReq: Route = {
    parseEntity(customJson(p => PublishApi.decodeBatch(p))) {
      case Nil => complete(DiagnosticMessage.error(StatusCodes.BadRequest, "empty payload"))
      case dps: List[Datapoint] =>
        if (localFileService.isHealthy) {
          localFileService.persist(dps)
          complete(HttpResponse(StatusCodes.OK))
        } else {
          complete(DiagnosticMessage.error(StatusCodes.ServiceUnavailable, "service unhealthy"))
        }
    }
  }
}
