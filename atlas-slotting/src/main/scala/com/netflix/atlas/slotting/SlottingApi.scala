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

import java.net.InetAddress

import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
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
      path("autoScalingGroups") {
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
      path("autoScalingGroups" / Remaining) { asgName =>
        get {
          complete(singleItem(slottingCache, asgName))
        }
      }
    } ~
    // edda compatibility endpoints
    pathPrefix(("api" | "REST") / "v2" / "group") {
      path("autoScalingGroups") {
        get {
          complete(indexList(slottingCache))
        }
      } ~
      path(autoScalingGroupsExpand) { _ =>
        get {
          complete(verboseList(slottingCache))
        }
      } ~
      path("autoScalingGroups" / Remaining) { asgNameWithArgs =>
        val asgName = stripEddaArgs.replaceAllIn(asgNameWithArgs, "")
        get {
          complete(singleItem(slottingCache, asgName))
        }
      }
    } ~
    pathEndOrSingleSlash {
      complete(mkResponse(StatusCodes.OK, serviceDescription))
    }
  }
}

object SlottingApi {

  val serviceDescription: Map[String, Any] = {
    val instanceId = Option(System.getenv("EC2_INSTANCE_ID"))

    val baseUrl =
      if (instanceId.isEmpty) {
        "http://localhost:7101"
      } else {
        val localhost: InetAddress = InetAddress.getLocalHost
        val localIpAddress: String = localhost.getHostAddress
        s"http://$localIpAddress"
      }

    Map(
      "description" -> "Atlas Slotting Service",
      "endpoints" -> List(
        s"$baseUrl/healthcheck",
        s"$baseUrl/api/v1/autoScalingGroups",
        s"$baseUrl/api/v1/autoScalingGroups?verbose=true",
        s"$baseUrl/api/v1/autoScalingGroups/:name",
        s"$baseUrl/api/v2/group/autoScalingGroups",
        s"$baseUrl/api/v2/group/autoScalingGroups;_expand",
        s"$baseUrl/api/v2/group/autoScalingGroups/:name",
      )
    )
  }

  val autoScalingGroupsExpand: Regex = "autoScalingGroups(?:;_expand.*|;_pp.*)".r

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
    if (slottingCache.asgs.contains(asgName)) {
      mkResponse(StatusCodes.OK, slottingCache.asgs(asgName))
    } else {
      mkResponse(StatusCodes.NotFound, Map("message" -> "Not Found"))
    }
  }
}
