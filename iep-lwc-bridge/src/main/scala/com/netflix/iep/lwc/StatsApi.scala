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
package com.netflix.iep.lwc

import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.json.Json

/**
  * Dump the stats for the expressions flowing through the bridge.
  */
class StatsApi(evaluator: ExpressionsEvaluator) extends WebApi {

  override def routes: Route = {
    endpointPathPrefix("api" / "v1" / "stats") {
      get {
        val stats = Json.encode(evaluator.stats)
        val entity = HttpEntity(MediaTypes.`application/json`, stats)
        val response = HttpResponse(StatusCodes.OK, Nil, entity)
        complete(response)
      }
    }
  }
}
