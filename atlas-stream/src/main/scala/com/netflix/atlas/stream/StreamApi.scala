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
package com.netflix.atlas.stream

import java.time.Duration

import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Connection
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
          val dataSources = DataSources.of(new DataSource("_", atlasUri))
          processStream(dataSources)
        }
      }
    } ~
    endpointPath("stream") {
      post {
        parseEntity(json[List[DataSource]]) { dsList =>
          val dsListWithDefaultStep = dsList.map { ds =>
            if (ds.getStep == null) {
              //this will extract step from uri or else take default value
              new DataSource(ds.getId, ds.getUri)
            } else {
              ds
            }
          }
          val dataSources = new DataSources(dsListWithDefaultStep.toSet.asJava)
          processStream(dataSources)
        }
      }
    } ~
    endpointPath("api" / "v1" / "validate") {
      post {
        parseEntity(json[List[String]]) { uris =>
          val results = uris.map { uri =>
            val ds = new Evaluator.DataSource("_", Duration.ZERO, uri)
            val result = Try(evaluator.validate(ds)) match {
              case Success(_) => DiagnosticMessage.info("ok")
              case Failure(e) => DiagnosticMessage.error(e)
            }
            Map("uri" -> uri, "result" -> result)
          }
          val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(results))
          complete(HttpResponse(StatusCodes.OK, Nil, entity))
        }
      }
    }
  }

  private def processStream(dataSources: DataSources): Route = {
    val heartbeatSrc = Source
      .repeat(heartbeat)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)

    //repeat and throttle to keep the source alive, actual rate of stream is decided by step
    val src = Source
      .repeat(dataSources)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      .via(Flow.fromProcessor(evaluator.createStreamsProcessor))
      .map { messageEnvelope =>
        prefix ++ ByteString(Json.encode[MessageEnvelope](messageEnvelope)) ++ suffix
      }
      .merge(heartbeatSrc)

    val entity = HttpEntity(MediaTypes.`text/event-stream`, src)
    val headers = List(Connection("close"))
    complete(HttpResponse(StatusCodes.OK, headers = headers, entity = entity))
  }
}
