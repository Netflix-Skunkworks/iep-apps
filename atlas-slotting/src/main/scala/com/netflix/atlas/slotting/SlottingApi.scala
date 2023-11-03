/*
 * Copyright 2014-2023 Netflix, Inc.
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

import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.spectator.ipc.ServerGroup
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.caching.LfuCache
import org.apache.pekko.http.caching.scaladsl.Cache
import org.apache.pekko.http.caching.scaladsl.CachingSettings
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RequestContext
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.RouteResult
import org.apache.pekko.http.scaladsl.server.directives.CachingDirectives.*

import scala.util.matching.Regex

class SlottingApi(system: ActorSystem, slottingCache: SlottingCache)
    extends WebApi
    with StrictLogging {

  import SlottingApi.*

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
    innerRoutes
  }

  /**
    * Routes for GET requests without any encoding or caching. Primarily used to avoid caching
    * behavior when testing.
    */
  def innerRoutes: Route = {
    extractRequest { request =>
      val compress = useGzip(request)

      // standard endpoints
      pathPrefix("api" / "v1") {
        endpointPath("autoScalingGroups") {
          get {
            parameters("verbose".as[Boolean].?) { verbose =>
              if (verbose.contains(true)) {
                complete(verboseAsgList(compress, slottingCache))
              } else {
                complete(indexAsgList(compress, slottingCache))
              }
            }
          }
        } ~
        endpointPath("autoScalingGroups", Remaining) { asgName =>
          get {
            complete(singleAsg(compress, slottingCache, asgName))
          }
        } ~
        endpointPath("clusters", Remaining) { clusterName =>
          get {
            parameters("verbose".as[Boolean].?) { verbose =>
              if (verbose.contains(true)) {
                complete(verboseClusterList(compress, slottingCache, clusterName))
              } else {
                complete(indexClusterList(compress, slottingCache, clusterName))
              }
            }
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
}

object SlottingApi {

  val autoScalingGroupsExpand: Regex = "autoScalingGroups(?:;_expand.*|;_pp;_expand.*)".r

  val stripEddaArgs: Regex = "(?:;_.*|:\\(.*)".r

  private def useGzip(request: HttpRequest): Boolean = {
    request.headers.exists {
      case enc: `Accept-Encoding` => enc.encodings.exists(_.matches(HttpEncodings.gzip))
      case _                      => false
    }
  }

  def mkResponse(compress: Boolean, statusCode: StatusCode, data: Any): HttpResponse = {
    // We compress locally rather than relying on the encodeResponse directive to ensure the
    // cache will have a strict entity that can be reused.
    if (compress) {
      HttpResponse(
        statusCode,
        headers = List(`Content-Encoding`(HttpEncodings.gzip)),
        entity = HttpEntity(MediaTypes.`application/json`, Gzip.compressString(Json.encode(data)))
      )
    } else {
      HttpResponse(
        statusCode,
        entity = HttpEntity(MediaTypes.`application/json`, Json.encode(data))
      )
    }
  }

  def indexAsgList(compress: Boolean, slottingCache: SlottingCache): HttpResponse = {
    mkResponse(compress, StatusCodes.OK, slottingCache.asgs.keySet)
  }

  def verboseAsgList(compress: Boolean, slottingCache: SlottingCache): HttpResponse = {
    mkResponse(compress, StatusCodes.OK, slottingCache.asgs.values.toList)
  }

  def singleAsg(compress: Boolean, slottingCache: SlottingCache, asgName: String): HttpResponse = {
    slottingCache.asgs.get(asgName) match {
      case Some(slottedAsgDetails) =>
        mkResponse(compress, StatusCodes.OK, slottedAsgDetails)
      case None =>
        mkResponse(compress, StatusCodes.NotFound, Map("message" -> "Not Found"))
    }
  }

  def indexClusterList(
    compress: Boolean,
    slottingCache: SlottingCache,
    clusterName: String
  ): HttpResponse = {
    mkResponse(
      compress,
      StatusCodes.OK,
      slottingCache.asgs.keySet.filter { asgName =>
        ServerGroup.parse(asgName).cluster() == clusterName
      }
    )
  }

  def verboseClusterList(
    compress: Boolean,
    slottingCache: SlottingCache,
    clusterName: String
  ): HttpResponse = {
    mkResponse(
      compress,
      StatusCodes.OK,
      slottingCache.asgs.values.toList.filter { asg =>
        ServerGroup.parse(asg.name).cluster() == clusterName
      }
    )
  }

  def serviceDescription(request: HttpRequest): HttpResponse = {
    val scheme = request.uri.scheme
    val host = request.headers.filter(_.name == "Host").map(_.value).head

    mkResponse(
      useGzip(request),
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
          s"$scheme://$host/api/v2/group/autoScalingGroups/:name"
        )
      )
    )
  }
}
