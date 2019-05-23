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
package com.netflix.atlas.slotting

import akka.actor.ActorSystem
import akka.http.caching.LfuCache
import akka.http.caching.scaladsl.Cache
import akka.http.caching.scaladsl.CachingSettings
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteResult
import akka.http.scaladsl.server.directives.CachingDirectives._
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.util.matching.Regex

class SlottingApi @Inject()(system: ActorSystem, slottingCache: SlottingCache)
    extends WebApi
    with StrictLogging {

  import SlottingApi._

  private val keyerFunction: PartialFunction[RequestContext, Uri] = {
    case r: RequestContext => r.request.uri
  }

  private val lfuCache: Cache[Uri, RouteResult] = LfuCache(CachingSettings(system))

  /**
    * In general the results for a given GET call do not change that often, but are likely to
    * be refreshed frequently by many instances. The results will be briefly cached to avoid
    * the need for converting into a JSON payload and then compressing that payload.
    */
  override def routes: Route = cache(lfuCache, keyerFunction) {
    encodeResponse {
      innerRoutes
    }
  }

  /**
    * Routes for GET requests without any encoding or caching. Primarily used to avoid caching
    * behavior when testing.
    */
  def innerRoutes: Route = {
    // standard endpoints
    pathPrefix("api" / "v1") {
      endpointPath("autoScalingGroups") {
        get {
          parameters("verbose".as[Boolean].?) { verbose =>
            if (verbose.contains(true)) {
              complete(verboseList(slottingCache))
            } else {
              complete(indexList(slottingCache))
            }
          }
        }
      } ~
      endpointPath("autoScalingGroups", Remaining) { asgName =>
        get {
          complete(singleItem(slottingCache, asgName))
        }
      }
    } ~
    // edda compatibility endpoints
    pathPrefix(("api" | "REST") / "v2" / "group") {
      endpointPath("autoScalingGroups") {
        get {
          complete(indexList(slottingCache))
        }
      } ~
      path(autoScalingGroupsExpand) { _ =>
        get {
          complete(verboseList(slottingCache))
        }
      } ~
      endpointPath("autoScalingGroups", Remaining) { asgNameWithArgs =>
        val asgName = stripEddaArgs.replaceAllIn(asgNameWithArgs, "")
        get {
          complete(singleItem(slottingCache, asgName))
        }
      }
    } ~
    pathEndOrSingleSlash {
      extractRequest { request =>
        complete(serviceDescription(request))
      }
    }
  }
}

object SlottingApi {

  val autoScalingGroupsExpand: Regex = "autoScalingGroups(?:;_expand.*|;_pp;_expand.*)".r

  val stripEddaArgs: Regex = "(?:;_.*|:\\(.*)".r

  def mkResponse(statusCode: StatusCode, data: Any): HttpResponse = {
    HttpResponse(
      statusCode,
      entity = HttpEntity(MediaTypes.`application/json`, Json.encode(data))
    )
  }

  def indexList(slottingCache: SlottingCache): HttpResponse = {
    mkResponse(StatusCodes.OK, slottingCache.asgs.keySet)
  }

  def verboseList(slottingCache: SlottingCache): HttpResponse = {
    mkResponse(StatusCodes.OK, slottingCache.asgs.values.toList)
  }

  def singleItem(slottingCache: SlottingCache, asgName: String): HttpResponse = {
    slottingCache.asgs.get(asgName) match {
      case Some(slottedAsgDetails) =>
        mkResponse(StatusCodes.OK, slottedAsgDetails)
      case None =>
        mkResponse(StatusCodes.NotFound, Map("message" -> "Not Found"))
    }
  }

  def serviceDescription(request: HttpRequest): HttpResponse = {
    val scheme = request.uri.scheme
    val host = request.headers.filter(_.name == "Host").map(_.value).head

    mkResponse(
      StatusCodes.OK,
      Map(
        "description" -> "Atlas Slotting Service",
        "endpoints" -> List(
          s"$scheme://$host/healthcheck",
          s"$scheme://$host/api/v1/autoScalingGroups",
          s"$scheme://$host/api/v1/autoScalingGroups?verbose=true",
          s"$scheme://$host/api/v1/autoScalingGroups/:name",
          s"$scheme://$host/api/v2/group/autoScalingGroups",
          s"$scheme://$host/api/v2/group/autoScalingGroups;_expand",
          s"$scheme://$host/api/v2/group/autoScalingGroups/:name",
        )
      )
    )
  }
}
