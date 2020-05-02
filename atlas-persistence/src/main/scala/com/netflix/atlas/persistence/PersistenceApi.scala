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
import com.netflix.atlas.webapi.PublishApi

class PersistenceApi(localFileService: LocalFilePersistService) extends WebApi {

  override def routes: Route = {
    endpointPathPrefix("api" / "v1" / "persistence") {
      post {
        parseEntity(customJson(p => PublishApi.decodeList(p))) { datapoints =>
          datapoints match {
            case Nil => complete(DiagnosticMessage.error(StatusCodes.OK, "empty payload"))
            case _ => {
              datapoints.foreach(dp => { localFileService.save(dp) })
              complete(HttpResponse(StatusCodes.OK))
            }
          }
        }
      }
    }
  }
}
