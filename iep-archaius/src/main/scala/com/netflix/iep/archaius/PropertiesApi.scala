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
package com.netflix.iep.archaius

import java.io.StringWriter
import java.util.Properties

import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.HttpCharsets
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.json.Json
import com.netflix.frigga.Names

class PropertiesApi(
  val propContext: PropertiesContext,
  implicit val actorRefFactory: ActorRefFactory
) extends WebApi {

  def routes: Route = {
    endpointPath("api" / "v1" / "property") {
      get {
        parameter("asg") { asg =>
          extractRequest { request =>
            val cluster = Names.parseName(asg).getCluster
            if (propContext.initialized) {
              val props = propContext.getClusterProps(cluster)
              complete(encode(request, props))
            } else {
              complete(HttpResponse(StatusCodes.ServiceUnavailable))
            }
          }
        }
      }
    }
  }

  private def encode(request: HttpRequest, props: List[PropertiesApi.Property]): HttpResponse = {
    val useJson = request.headers.exists(h => h.is("accept") && h.value == "application/json")
    if (useJson) {
      HttpResponse(
        StatusCodes.OK,
        entity = HttpEntity(MediaTypes.`application/json`, Json.encode(props))
      )
    } else {
      val ps = new Properties
      props.foreach { p =>
        ps.setProperty(p.key, p.value)
      }
      val writer = new StringWriter()
      ps.store(writer, s"count: ${ps.size}")
      writer.close()
      val entity =
        HttpEntity(MediaTypes.`text/plain`.toContentType(HttpCharsets.`UTF-8`), writer.toString)
      HttpResponse(StatusCodes.OK, entity = entity)
    }
  }
}

object PropertiesApi {
  case class Property(id: String, cluster: String, key: String, value: String, timestamp: Long)
}
