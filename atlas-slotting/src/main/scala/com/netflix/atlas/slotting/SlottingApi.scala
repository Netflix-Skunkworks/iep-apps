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

import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.util.matching.Regex

class SlottingApi @Inject()(slottingCache: SlottingCache) extends WebApi with StrictLogging {

  import SlottingApi._

  override def routes: Route = {
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
