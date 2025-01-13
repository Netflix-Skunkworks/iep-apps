/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.ClusterConfig
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration.*
import scala.util.Success

class PurgerSuite extends FunSuite {

  import ExpressionDetails.*
  import PurgerImpl.*

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  test("Filter purge eligible expressions") {
    val data = List(
      ExpressionDetails(
        ExpressionId("cluster1", ForwardingExpression("", "", None, "")),
        1552891811000L,
        Nil,
        None,
        Map(NoDataFoundEvent -> (1552891811000L - 11.minutes.toMillis)),
        Nil
      ),
      ExpressionDetails(
        ExpressionId("cluster2", ForwardingExpression("", "", None, "")),
        1552891811000L,
        Nil,
        None,
        Map.empty[String, Long],
        Nil
      )
    )

    val dao = new ExpressionDetailsDaoTestImpl {
      override def read(
        id: ExpressionId
      ): Option[ExpressionDetails] = {
        data.find(_.expressionId == id)
      }
      override def isPurgeEligible(
        ed: ExpressionDetails,
        now: Long
      ): Boolean = {
        ed.expressionId.key == "cluster1"
      }
    }

    val future = Source(data.map(_.expressionId).groupBy(_.key))
      .via(filterPurgeEligibleExprs(dao))
      .runWith(Sink.seq)

    val actual = Await.result(future, Duration.Inf)
    val expected = data.filter(_.expressionId.key == "cluster1").groupBy(_.expressionId.key).toSeq

    assertEquals(actual, expected)
  }

  test("Read cluster config") {

    val cfgPayload = ClusterConfig("", List(ForwardingExpression("uri", "", None, "")))

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

    assertEquals(actual, cfgPayload)
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

    assertEquals(actual, None)
  }

  test("Update cluster config") {
    val expressions = List(
      ForwardingExpression("uri1", "", None, ""),
      ForwardingExpression("uri2", "", None, ""),
      ForwardingExpression("uri2", "", None, "")
    )

    val cfgPayload = ClusterConfig("", expressions)

    val now = 1552891811000L
    val exprToRemove = List(
      ExpressionDetails(
        ExpressionId("cluster1", expressions.find(_.atlasUri == "uri1").get),
        1552891811000L,
        Nil,
        None,
        Map(NoDataFoundEvent -> (now - 11.minutes.toMillis)),
        Nil
      )
    )

    val actual = makeClusterCfgPayload(exprToRemove, cfgPayload)

    val expected = ClusterConfig("", expressions.filterNot(_.atlasUri == "uri1"))

    assertEquals(actual, expected)
  }

  test("Send out POST req for purging expr in cluster config") {
    val requests = List.newBuilder[HttpRequest]
    val client: Client =
      Flow[(HttpRequest, AccessLogger)]
        .map {
          case (request, accessLogger) =>
            requests += request
            (Success(HttpResponse(StatusCodes.OK)), accessLogger)
        }

    val cfgPayload = ClusterConfig("", List(ForwardingExpression("", "", None, "")))

    val future = Source
      .single(("config1", cfgPayload))
      .via(doPurge("http://local/%s", client))
      .runWith(Sink.head)

    val expected = HttpRequest(
      HttpMethods.POST,
      Uri("http://local/config1"),
      entity = Json.encode(cfgPayload)
    ).withHeaders(RawHeader("conditional", "true"))

    val result = Await.result(future, Duration.Inf)

    assertEquals(result, NotUsed)
    assertEquals(requests.result(), List(expected))
  }

  test("Remove expression details") {
    val dao = new ExpressionDetailsDaoTestImpl

    val ed = ExpressionDetails(
      ExpressionId("", ForwardingExpression("", "", None, "", Nil)),
      1552891811000L,
      Nil,
      None,
      Map.empty[String, Long],
      Nil
    )

    val future = Source
      .single(ed)
      .via(removeExprDetails(dao))
      .runWith(Sink.head)

    val actual = Await.result(future, Duration.Inf)
    assertEquals(actual, NotUsed)
  }

}
