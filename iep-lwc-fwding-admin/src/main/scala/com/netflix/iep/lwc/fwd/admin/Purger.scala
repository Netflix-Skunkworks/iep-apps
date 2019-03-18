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
import akka.stream.ActorAttributes
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
  expressionDetailsDao: ExpressionDetailsDao,
  implicit val system: ActorSystem
) extends Purger
    with StrictLogging {
  import PurgerImpl._

  private implicit val mat = ActorMaterializer()

  private val cwExprUri = config.getString("iep.lwc.fwding-admin.cw-expr-uri")
  private val client = Http().superPool[AccessLogger]()

  override def purge(expressions: List[ExpressionId]): Future[Done] = {
    Source(expressions.groupBy(_.key))
      .via(filterPurgeEligibleExprs(expressionDetailsDao))
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
            (k, e),
            makeClusterCfgPayload(e, c)
          )
      }
      .flatMapConcat {
        case ((k, e), c) =>
          Source
            .single((k, c))
            .via(doPurge(cwExprUri, client))
            .map(_ => e)
      }
      .mapConcat(identity)
      .via(removeExprDetails(expressionDetailsDao))
      .runWith(Sink.ignore)
  }

}

object PurgerImpl extends StrictLogging {

  val BlockingDispatcher = "blocking-dispatcher"

  type Client = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]

  def filterPurgeEligibleExprs(
    expressionDetailsDao: ExpressionDetailsDao
  ): Flow[(String, List[ExpressionId]), (String, List[ExpressionDetails]), NotUsed] = {
    Flow[(String, List[ExpressionId])]
      .flatMapConcat {
        case (k, e) =>
          Source(e)
            .via(readExprDetails(expressionDetailsDao))
            .filter(expressionDetailsDao.isPurgeEligible(_, System.currentTimeMillis()))
            .fold(List.empty[ExpressionDetails])(_ :+ _)
            .map((k, _))
      }
      .filter { case (_, v) => v.nonEmpty }
  }

  def readExprDetails(
    expressionDetailsDao: ExpressionDetailsDao
  ): Flow[ExpressionId, ExpressionDetails, NotUsed] = {

    def read(id: ExpressionId): Try[Option[ExpressionDetails]] = {
      val result = Try(expressionDetailsDao.read(id))
      result match {
        case Failure(e) =>
          logger.error(s"Error reading from DynamoDB", e)
        case _ =>
      }
      result
    }

    Flow[ExpressionId]
      .map(read(_))
      .withAttributes(ActorAttributes.dispatcher(BlockingDispatcher))
      .collect { case Success(Some(ed)) => ed }
  }

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

  def makeClusterCfgPayload(
    exprToRemove: List[ExpressionDetails],
    clusterCfgPayload: ClusterCfgPayload
  ): ClusterCfgPayload = {

    val markers = exprToRemove.flatMap(_.events.keys).toSet.mkString(",")
    val comment = s"Removed ${exprToRemove.size} expressions. Purge Markers: $markers"

    ClusterCfgPayload(
      clusterCfgPayload.version.copy(user = Some("admin"), comment = Some(comment)),
      clusterCfgPayload.payload.copy(
        expressions = clusterCfgPayload.payload.expressions.diff(
          exprToRemove.map(_.expressionId.expression)
        )
      )
    )
  }

  def doPurge(
    cwExprUri: String,
    client: Client
  ): Flow[(String, ClusterCfgPayload), NotUsed, NotUsed] = {
    Flow[(String, ClusterCfgPayload)]
      .map {
        case (k, c) =>
          HttpRequest(
            HttpMethods.PUT,
            Uri(cwExprUri.format(k)),
            entity = Json.encode(c)
          ).withHeaders(RawHeader("conditional", "true"))
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

  def removeExprDetails(
    expressionDetailsDao: ExpressionDetailsDao
  ): Flow[ExpressionDetails, NotUsed, NotUsed] = {
    def delete(id: ExpressionId): Try[Unit] = {
      val result = Try(expressionDetailsDao.delete(id))
      result match {
        case Failure(e) =>
          logger.error(s"Error removing from DynamoDB", e)
        case _ =>
      }
      result
    }

    Flow[ExpressionDetails]
      .map(ed => delete(ed.expressionId))
      .withAttributes(ActorAttributes.dispatcher(BlockingDispatcher))
      .collect { case Success(result) => result }
      .map(_ => NotUsed)
  }

}

case class ClusterCfgPayload(
  version: ConfigBinVersion,
  payload: ClusterConfig
)
