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

import java.util.concurrent.TimeUnit._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.SourceQueue
import com.netflix.iep.aws.AwsClientFactory
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.Report
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait MarkerService {
  var queue: SourceQueue[Report]
}

class MarkerServiceImpl @Inject()(
  config: Config,
  registry: Registry,
  clientFactory: AwsClientFactory,
  expressionDetailsDao: ExpressionDetailsDao,
  implicit val system: ActorSystem
) extends AbstractService
    with MarkerService
    with StrictLogging {

  import MarkerServiceImpl._

  private implicit val mat = ActorMaterializer()
  private var killSwitch: KillSwitch = _

  override var queue: SourceQueue[Report] = _

  override def startImpl(): Unit = {
    val (q, k) = Source
      .queue[Report](10, OverflowStrategy.dropNew)
      .via(readExprDetails(expressionDetailsDao))
      .map { case (r, e) => toExprDetails(r, e) }
      .via(saveExprDetails(expressionDetailsDao))
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.ignore)(Keep.left)
      .run()

    queue = q
    killSwitch = k

    logger.info("MarkerService started")
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }
}

object MarkerServiceImpl extends StrictLogging {

  val BlockingDispatcher = "blocking-dispatcher"

  def readExprDetails(
    expressionDetailsDao: ExpressionDetailsDao
  ): Flow[Report, (Report, Option[ExpressionDetails]), NotUsed] = {

    def read(id: ExpressionId): Try[Option[ExpressionDetails]] = {
      val result = Try(expressionDetailsDao.read(id))
      result match {
        case Failure(e) =>
          logger.error(s"Error reading from DynamoDB", e)
        case _ =>
      }
      result
    }

    Flow[Report]
      .map(r => (r, read(r.id)))
      .withAttributes(ActorAttributes.dispatcher(BlockingDispatcher))
      .collect { case (r, Success(ed)) => (r, ed) }
  }

  def toExprDetails(report: Report, exprDetails: Option[ExpressionDetails]): ExpressionDetails = {
    val ed = ExpressionDetails(report.id, report.timestamp, report.metric, report.error, 0, 0, None)

    exprDetails
      .map { e =>
        val noDataAgeMins = report.metric
          .map(_ => 0L)
          .getOrElse(
            e.noDataAgeMins + Duration(
              (report.timestamp - e.lastReportTs),
              MILLISECONDS
            ).toMinutes
          )
          .toInt

        ed.copy(noDataAgeMins = noDataAgeMins)
      }
      .getOrElse(ed)
  }

  def saveExprDetails(
    expressionDetailsDao: ExpressionDetailsDao
  ): Flow[ExpressionDetails, NotUsed, NotUsed] = {

    def save(ed: ExpressionDetails): Try[Unit] = {
      val result = Try(expressionDetailsDao.save(ed))
      result match {
        case Failure(e) =>
          logger.error(s"Error saving to DynamoDB", e)
        case _ =>
      }
      result
    }

    Flow[ExpressionDetails]
      .map(save(_))
      .withAttributes(ActorAttributes.dispatcher(BlockingDispatcher))
      .collect { case Success(result) => result }
      .map(_ => NotUsed)
  }
}
