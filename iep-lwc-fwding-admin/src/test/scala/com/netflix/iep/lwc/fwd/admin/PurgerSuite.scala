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
package com.netflix.iep.lwc.fwd.admin
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.ClusterConfig
import com.netflix.iep.lwc.fwd.cw.ConfigBinVersion
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Success

class PurgerSuite extends FunSuite {

  import PurgerImpl._

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  test("Read cluster config") {

    val cfgPayload = ClusterCfgPayload(
      ConfigBinVersion(1552669178072L, "", None, None),
      ClusterConfig("", List(ForwardingExpression("", "", None, "")))
    )

    val client: Client =
      Flow[(HttpRequest, AccessLogger)]
        .map {
          case (_, accessLogger) =>
            (
              Success(
                HttpResponse(
                  StatusCodes.OK,
                  entity = HttpEntity(MediaTypes.`application/json`, Json.encode(cfgPayload))
                )
              ),
              accessLogger
            )
        }

    val future = Source
      .single("config1")
      .via(getClusterConfig("http://local/%s", client))
      .runWith(Sink.head)

    val actual = Await.result(future, Duration.Inf)

    assert(actual === cfgPayload)
  }

  test("Read cluster config that is not found") {

    val client: Client =
      Flow[(HttpRequest, AccessLogger)]
        .map {
          case (_, accessLogger) =>
            (
              Success(HttpResponse(StatusCodes.NotFound)),
              accessLogger
            )
        }

    val future = Source
      .single("config1")
      .via(getClusterConfig("http://local/%s", client))
      .runWith(Sink.headOption)

    val actual = Await.result(future, Duration.Inf)

    assert(actual === None)
  }

  test("Update cluster config") {
    val expressions = List(
      ForwardingExpression("uri1", "", None, ""),
      ForwardingExpression("uri2", "", None, ""),
      ForwardingExpression("uri2", "", None, "")
    )

    val clusterConfig = ClusterConfig("", expressions)

    val toPurge = expressions.filter(_.atlasUri == "uri1")
    val actual = removeExpressions(toPurge, clusterConfig)

    val expected = expressions.filterNot(_.atlasUri == "uri1")
    assert(actual === Some(ClusterConfig("", expected)))
  }

  test("Remove cluster config") {
    val exprs = List(ForwardingExpression("uri1", "", None, ""))
    val clusterConfig = ClusterConfig("", exprs)

    val actual = removeExpressions(exprs, clusterConfig)

    assert(actual === None)
  }

  test("Send out PUT req for purging expr in cluster config") {
    val requests = List.newBuilder[HttpRequest]
    val client: Client =
      Flow[(HttpRequest, AccessLogger)]
        .map {
          case (request, accessLogger) =>
            requests += request
            (Success(HttpResponse(StatusCodes.OK)), accessLogger)
        }

    val cfgPayload = ClusterCfgPayload(
      ConfigBinVersion(1552669178072L, "", None, None),
      ClusterConfig("", List(ForwardingExpression("", "", None, "")))
    )

    val future = Source
      .single(("config1", Some(cfgPayload)))
      .via(doPurge("http://local/%s", client))
      .runWith(Sink.head)

    val expected = HttpRequest(
      HttpMethods.PUT,
      Uri("http://local/config1"),
      entity = Json.encode(cfgPayload)
    ).withHeaders(RawHeader("conditional", "true"))

    val result = Await.result(future, Duration.Inf)

    assert(result === NotUsed)
    assert(requests.result() === List(expected))
  }

  test("Send out DELETE req for removing a cluster config") {
    val requests = List.newBuilder[HttpRequest]
    val client: Client =
      Flow[(HttpRequest, AccessLogger)]
        .map {
          case (request, accessLogger) =>
            requests += request
            (Success(HttpResponse(StatusCodes.OK)), accessLogger)
        }

    val future = Source
      .single(("config1", None))
      .via(doPurge("http://local/%s", client))
      .runWith(Sink.head)

    val expected = HttpRequest(
      HttpMethods.DELETE,
      Uri("http://local/config1?user=admin&comment=cleanup"),
    )

    val result = Await.result(future, Duration.Inf)

    assert(result === NotUsed)
    assert(requests.result() === List(expected))
  }

}
