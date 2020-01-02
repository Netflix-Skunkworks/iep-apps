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
package com.netflix.iep.lwc.fwd.admin
import akka.NotUsed
import akka.actor.Actor
import akka.actor.Timers
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.iep.NetflixEnvironment._
import com.netflix.iep.lwc.fwd.admin.ScalingPolicies._
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.util.Failure

class ScalingPolicies(config: Config, dao: ScalingPoliciesDao)
    extends Actor
    with Timers
    with StrictLogging {

  private implicit val mat = ActorMaterializer()
  private implicit val ec = scala.concurrent.ExecutionContext.global

  protected var scalingPolicies = Map.empty[EddaEndpoint, List[ScalingPolicy]]
  private val accountEnvMapping =
    config.getObject("iep.lwc.fwding-admin.accountEnvMapping").toConfig

  private val cacheRefreshInterval =
    config.getDuration("iep.lwc.fwding-admin.edda-cache-refresh-interval")

  startPeriodicTimer()

  override def receive: Receive = {
    case GetScalingPolicy(metricInfo) => respond(getScalingPolicy(metricInfo))
    case AddScalingPolicies(policies) => scalingPolicies = scalingPolicies ++ policies
    case RefreshCache                 => respond(refreshCache)
    case GetCache                     => sender() ! scalingPolicies
    case message                      => throw new RuntimeException(s"Unknown message ${message}")
  }

  def startPeriodicTimer(): Unit = {
    timers.startPeriodicTimer(NotUsed, RefreshCache, cacheRefreshInterval)
  }

  def respond[T](handleMessage: => Future[T]): Unit = {
    val senderRef = sender()
    val future = handleMessage.map { response =>
      senderRef ! response
    }

    future.onComplete {
      case Failure(e) => logger.error("Error processing message", e)
      case _          =>
    }
  }

  def getScalingPolicy(metricInfo: FwdMetricInfo): Future[Option[ScalingPolicy]] = {
    getScalingPolicies(
      EddaEndpoint(
        metricInfo.account,
        metricInfo.region,
        getEnv(metricInfo.account)
      )
    ).map(_.find(_.matchMetric(metricInfo)))
  }

  def getEnv(account: String): String = {
    getEnvMapping(account).getOrElse(env())
  }

  def getEnvMapping(account: String): Option[String] = {
    if (accountEnvMapping.hasPath(account)) {
      Some(accountEnvMapping.getString(account))
    } else {
      None
    }
  }

  def getScalingPolicies(
    eddaEndpoint: EddaEndpoint
  ): Future[List[ScalingPolicy]] = {
    scalingPolicies
      .get(eddaEndpoint)
      .map(Future(_))
      .getOrElse {
        Source
          .single(eddaEndpoint)
          .via(dao.getScalingPolicies())
          .runWith(Sink.headOption)
          .map { policies =>
            policies
              .map { p =>
                self ! AddScalingPolicies(Map(eddaEndpoint -> p))
                p
              }
              .getOrElse(throw new RuntimeException("Edda Error"))
          }
      }
  }

  def refreshCache(): Future[Unit] = {
    val eddaEndpoints = scalingPolicies.keys.toList
    Source(eddaEndpoints)
      .flatMapConcat { eddaEndpoint =>
        Source
          .single(eddaEndpoint)
          .via(dao.getScalingPolicies())
          .map((eddaEndpoint, _))
      }
      .runWith(Sink.seq)
      .map { allPolicies =>
        if (allPolicies.size != eddaEndpoints.size) {
          val failedEndpoints = eddaEndpoints.diff(
            allPolicies
              .map { case (e, _) => e }
          )
          logger.error(s"Failed to refresh Edda for endpoints: $failedEndpoints")
        }
        self ! AddScalingPolicies(allPolicies.toMap)
      }
  }
}

object ScalingPolicies {
  case class GetScalingPolicy(metricInfo: FwdMetricInfo)
  case object GetCache

  private case class AddScalingPolicies(policies: Map[EddaEndpoint, List[ScalingPolicy]])
  private[admin] case object RefreshCache
}
