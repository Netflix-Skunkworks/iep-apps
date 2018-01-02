/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.actor.ActorRefFactory
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteResult
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.webapi.PublishApi

import scala.concurrent.Promise

/**
  * Mimics an internal update API that is sometimes used instead of publish.
  */
class UpdateApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val publishRef = actorRefFactory.actorSelection("/user/publish")

  override def routes: Route = {
    pathPrefix("database" / "v1" / "update") {
      post {
        extractRequestContext { ctx =>
          parseEntity(customJson(p => PublishApi.decodeList(p))) { datapoints =>
            val promise = Promise[RouteResult]()
            publishRef ! PublishApi.PublishRequest(null, datapoints, Nil, promise, ctx)
            _ => promise.future
          }
        }
      }
    }
  }
}
