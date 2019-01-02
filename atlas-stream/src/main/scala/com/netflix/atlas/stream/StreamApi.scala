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
package com.netflix.atlas.stream

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Connection
import akka.http.scaladsl.server.Route
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.stream.Evaluator

import scala.concurrent.duration._

class StreamApi(evaluator: Evaluator) extends WebApi {

  private val prefix = ByteString("data: ")
  private val suffix = ByteString("\r\n\r\n")

  private val heartbeat = ByteString(s"""data: {"type":"heartbeat"}\r\n\r\n""")

  def routes: Route = {
    endpointPath("stream", RemainingPath) { path =>
      get {
        extractUri { uri =>
          val q = uri.rawQueryString.getOrElse("")
          val atlasUri = s"$path?$q"

          val heartbeatSrc = Source
            .repeat(heartbeat)
            .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)

          val src = Source
            .fromPublisher(evaluator.createPublisher(atlasUri))
            .map { obj =>
              prefix ++ ByteString(obj.toJson) ++ suffix
            }
            .merge(heartbeatSrc)
          val entity = HttpEntity(MediaTypes.`text/event-stream`, src)
          val headers = List(Connection("close"))
          complete(HttpResponse(StatusCodes.OK, headers = headers, entity = entity))
        }
      }
    }
  }
}
