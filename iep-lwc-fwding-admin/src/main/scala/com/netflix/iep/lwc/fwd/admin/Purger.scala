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
import java.nio.charset.StandardCharsets

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw._
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Purger {
  def purge(expressions: List[ExpressionId]): Future[Done]
}

class PurgerImpl @Inject()(
  config: Config,
  implicit val system: ActorSystem
) extends Purger
    with StrictLogging {
  import PurgerImpl._

  private implicit val mat = ActorMaterializer()

  private val cwExprUri = config.getString("iep.lwc.fwding-admin.cw-expr-uri")
  private val client = Http().superPool[AccessLogger]()

  override def purge(expressions: List[ExpressionId]): Future[Done] = {

    Source(expressions.groupBy(_.key))
      .flatMapConcat { exprGrp =>
        val (k, _) = exprGrp
        Source
          .single(k)
          .via(getClusterConfig(cwExprUri, client))
          .map(out => (exprGrp, out))

      }
      .map {
        case ((k, e), c) =>
          (
            k,
            removeExpressions(
              e.map(_.expression),
              c.payload
            ).map(
              ClusterCfgPayload(
                c.version.copy(user = Some("admin"), comment = Some("Cleanup")),
                _
              )
            )
          )
      }
      .via(doPurge(cwExprUri, client))
      .runWith(Sink.ignore)
  }

}

object PurgerImpl extends StrictLogging {

  type Client = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]

  def getClusterConfig(
    cwExprUri: String,
    client: Client
  ): Flow[String, ClusterCfgPayload, NotUsed] = {
    Flow[String]
      .map(key => HttpRequest(HttpMethods.GET, Uri(cwExprUri.format(key))))
      .map(r => r -> AccessLogger.newClientLogger("configbin", r))
      .via(client)
      .map {
        case (result, accessLog) =>
          accessLog.complete(result)

          result match {
            case Failure(e) => logger.error("Reading cw expression failed", e)
            case _          =>
          }

          result
      }
      .collect { case Success(r) => r }
      .filter(_.status == StatusCodes.OK)
      .flatMapConcat(r => r.entity.dataBytes)
      .map { d =>
        Json
          .decode[ClusterCfgPayload](
            d.decodeString(StandardCharsets.UTF_8)
          )
      }
  }

  def removeExpressions(
    expressions: List[ForwardingExpression],
    clusterCfg: ClusterConfig
  ): Option[ClusterConfig] = {
    val e = clusterCfg.expressions.diff(expressions)

    if (e.nonEmpty) {
      Some(clusterCfg.copy(expressions = e))
    } else {
      None
    }
  }

  def doPurge(
    cwExprUri: String,
    client: Client
  ): Flow[(String, Option[ClusterCfgPayload]), NotUsed, NotUsed] = {
    Flow[(String, Option[ClusterCfgPayload])]
      .map {
        case (k, c) =>
          c.map { cfg =>
              HttpRequest(
                HttpMethods.PUT,
                Uri(cwExprUri.format(k)),
                entity = Json.encode(cfg)
              ).withHeaders(RawHeader("conditional", "true"))
            }
            .getOrElse(
              HttpRequest(
                HttpMethods.DELETE,
                Uri(s"${cwExprUri.format(k)}?user=admin&comment=cleanup")
              )
            )
      }
      .map { r =>
        r -> AccessLogger.newClientLogger("configbin", r)
      }
      .via(client)
      .via(StreamOps.map { (t, mat) =>
        val (result, accessLog) = t
        accessLog.complete(result)
        result match {
          case Success(response) =>
            response.discardEntityBytes()(mat)
          case Failure(e) =>
            logger.error(s"Purging cw expression failed", e)
        }
        NotUsed
      })
  }

}

case class ClusterCfgPayload(
  version: ConfigBinVersion,
  payload: ClusterConfig
)
