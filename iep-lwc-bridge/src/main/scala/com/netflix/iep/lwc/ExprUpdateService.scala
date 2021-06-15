/*
 * Copyright 2014-2021 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicLong
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.OverflowStrategy
import akka.stream.RestartSettings
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RestartFlow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.ByteStringInputStream
import com.netflix.atlas.json.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.netflix.spectator.atlas.impl.Subscriptions
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import javax.inject.Inject
import scala.concurrent.Future
import scala.util.Success

/**
  * Refresh the set of expressions from the LWC service.
  */
class ExprUpdateService @Inject()(
  config: Config,
  registry: Registry,
  evaluator: ExpressionsEvaluator,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  import scala.concurrent.duration._
  import com.netflix.atlas.akka.OpportunisticEC._

  private val configUri = Uri(config.getString("netflix.iep.lwc.bridge.config-uri"))

  private val lastUpdateTime = PolledMeter
    .using(registry)
    .withName("lwc.expressionsAge")
    .monitorValue(new AtomicLong(registry.clock().wallTime()), Functions.age(registry.clock()))

  private val syncPayloadBytes = registry.distributionSummary("lwc.syncPayloadBytes")
  private val syncPayloadExprs = registry.distributionSummary("lwc.syncPayloadExprs")

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    val request = HttpRequest(HttpMethods.GET, configUri)
    val client = Http().superPool[AccessLogger]()
    val flow = Flow[HttpRequest]
      .map { req =>
        request -> AccessLogger.newClientLogger("lwc-subs", req)
      }
      .via(client)
      .map {
        case (result, accessLog) =>
          accessLog.complete(result)
          result
      }
      .collect {
        case Success(response) => response
      }
      .via(syncExpressionsFlow)
    val restartSettings = RestartSettings(5.seconds, 5.seconds, 1.0)
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings) { () =>
      flow
    }
    killSwitch = Source
      .repeat(request)
      .throttle(1, 10.seconds, 1, ThrottleMode.Shaping)
      .via(restartFlow)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  private[lwc] def syncExpressionsFlow: Flow[HttpResponse, NotUsed, NotUsed] = {
    Flow[HttpResponse]
      .flatMapConcat {
        case response if response.status == StatusCodes.OK =>
          response.entity
            .withoutSizeLimit()
            .dataBytes
            .reduce(_ ++ _)
        case response =>
          response.discardEntityBytes()
          Source.single(ByteString.empty)
      }
      .map { bytes =>
        syncPayloadBytes.record(bytes.size)
        bytes
      }
      .filterNot(_.isEmpty)
      .buffer(1, OverflowStrategy.dropHead)
      .flatMapConcat { data =>
        Source.future(update(data))
      }
  }

  private def update(data: ByteString): Future[NotUsed] = {
    import scala.jdk.CollectionConverters._
    Future {
      try {
        val exprs = Json
          .decode[Subscriptions](new ByteStringInputStream(data))
          .getExpressions
          .asScala
          .filter(_.getFrequency == 60000) // Limit to primary publish step size
          .asJava
        evaluator.sync(exprs)
        syncPayloadExprs.record(exprs.size())
        lastUpdateTime.set(registry.clock().wallTime())
      } catch {
        case e: Exception =>
          logger.error("failed to parse and sync expressions", e)
      }
      NotUsed
    }
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }
}
