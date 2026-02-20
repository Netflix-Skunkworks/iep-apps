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
package com.netflix.atlas.druid

import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.RouteResult
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.druid.ExplainApi.ExplainRequest
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.atlas.json3.Json
import com.netflix.atlas.webapi.GraphApi.DataRequest
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class ExplainApi(config: Config, implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val grapher: Grapher = Grapher(config)

  private val dbRef = actorRefFactory.actorSelection("/user/db")

  private implicit val ec: ExecutionContext = actorRefFactory.dispatcher

  override def routes: Route = {
    endpointPath("explain" / "v1" / "graph") {
      get { ctx =>
        val graphCfg = grapher.toGraphConfig(ctx.request)
        dbRef
          .ask(ExplainRequest(DataRequest(graphCfg)))(Timeout(10.seconds))
          .map { response =>
            val json = Json.encode(response)
            val entity = HttpEntity(MediaTypes.`application/json`, json)
            RouteResult.Complete(HttpResponse(StatusCodes.OK, entity = entity))
          }
      }
    }
  }
}

object ExplainApi {
  case class ExplainRequest(dataRequest: DataRequest)
}
