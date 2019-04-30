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

import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.StreamOps.SourceQueue
import com.netflix.iep.aws.AwsClientFactory
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.Report
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

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
  private implicit val ec = scala.concurrent.ExecutionContext.global
  private var killSwitch: KillSwitch = _

  override var queue: SourceQueue[Report] = _

  override def startImpl(): Unit = {
    val sizeLimit = config.getInt("iep.lwc.fwding-admin.queue-size-limit")
    val scalingPolicies = system.actorSelection("/user/scalingPolicies")

    val (q, k) = StreamOps
      .blockingQueue[Report](registry, "fwdingAdminCwReports", sizeLimit)
      .flatMapConcat { r =>
        Source
          .single(r)
          .via(readExprDetails(expressionDetailsDao))
          .map(e => (r, e))
      }
      .flatMapConcat {
        case (r, e) =>
          Source
            .single(r)
            .via(lookupScalingPolicy(scalingPolicies))
            .map(s => (r, s, e))
      }
      .map { case (r, s, e) => toExprDetails(r, s, e) }
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

  def lookupScalingPolicy(
    scalingPolicies: ActorSelection
  )(implicit ec: ExecutionContext): Flow[Report, ScalingPolicyStatus, NotUsed] = {
    import ScalingPolicies._
    implicit val askTimeout = Timeout(15.seconds)

    Flow[Report]
      .mapAsync(1) { r =>
        r.metric
          .map { metricInfo =>
            (scalingPolicies ? GetScalingPolicy(metricInfo))
              .mapTo[Option[ScalingPolicy]]
              .map(ScalingPolicyStatus(false, _))
              .recover {
                case _: TimeoutException =>
                  logger.error(s"Looking up scaling policy timed out for $metricInfo")
                  ScalingPolicyStatus(true, None)
              }
          }
          .getOrElse(Future(ScalingPolicyStatus(true, None)))
      }
  }

  def readExprDetails(
    expressionDetailsDao: ExpressionDetailsDao
  ): Flow[Report, Option[ExpressionDetails], NotUsed] = {

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
      .map(r => read(r.id))
      .withAttributes(ActorAttributes.dispatcher(BlockingDispatcher))
      .collect { case Success(ed) => ed }
  }

  def toExprDetails(
    report: Report,
    scalingPolicyStatus: ScalingPolicyStatus,
    prevExprDetails: Option[ExpressionDetails]
  ): ExpressionDetails = {
    import ExpressionDetails._

    val noDataFoundEvent = report.metric
      .map(_ => Map.empty[String, Long])
      .getOrElse(
        Map(
          NoDataFoundEvent -> prevExprDetails
            .flatMap(_.events.get(NoDataFoundEvent))
            .getOrElse(report.timestamp)
        )
      )

    val noScalingPolicyFoundEvent =
      if (scalingPolicyStatus.unknown || scalingPolicyStatus.scalingPolicy.isDefined) {
        Map.empty[String, Long]
      } else {
        Map(
          NoScalingPolicyFoundEvent -> prevExprDetails
            .flatMap(_.events.get(NoScalingPolicyFoundEvent))
            .getOrElse(report.timestamp)
        )
      }

    ExpressionDetails(
      report.id,
      report.timestamp,
      addOrMake(report.metric, prevExprDetails, _.forwardedMetrics),
      report.error,
      noDataFoundEvent ++ noScalingPolicyFoundEvent,
      addOrMake(scalingPolicyStatus.scalingPolicy, prevExprDetails, _.scalingPolicies)
    )
  }

  def addOrMake[T](
    item: Option[T],
    prev: Option[ExpressionDetails],
    getList: ExpressionDetails => List[T]
  ): List[T] = {
    val prevItems = prev.fold(List.empty[T])(getList)
    item.foldLeft(prevItems) { (items, i) =>
      (i :: items).distinct
    }
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
