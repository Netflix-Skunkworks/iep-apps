/*
 * Copyright 2014-2017 Netflix, Inc.
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

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.histogram.BucketFunctions
import com.netflix.spectator.atlas.impl.Evaluator
import com.netflix.spectator.atlas.impl.Subscription
import com.netflix.spectator.atlas.impl.TagsValuePair
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future


/**
  * Takes messages from publish API handler and forwards them to LWC.
  */
class LwcPublishActor(config: Config, registry: Registry, evaluator: Evaluator)
  extends Actor with StrictLogging {

  import ExprUpdateActor._
  import com.netflix.atlas.webapi.PublishApi._

  import scala.concurrent.ExecutionContext.Implicits.global

  type SubscriptionList = java.util.List[Subscription]

  private implicit val materializer = ActorMaterializer()

  private val evalUri = Uri(config.getString("netflix.iep.lwc.bridge.eval-uri"))

  private val step = DefaultSettings.stepSize

  // Number of invalid datapoints received
  private val numReceivedCounter = BucketCounter.get(
    registry,
    registry.createId("atlas.db.numMetricsReceived"),
    BucketFunctions.ageBiasOld(step, TimeUnit.MILLISECONDS))

  // Number of invalid datapoints received
  private val numInvalidId = registry.createId("atlas.db.numInvalid")

  // Note: this will always use a 200 response type. The assumed use-case is a proxy
  // that tees the requests to both a typical publish endpoint and this cluster. The user
  // response should come from the publish endpoint with the response here merely acknowledging
  // receipt. For now to avoid spurious retries and confusion this response is always OK.
  def receive: Receive = {
    case req @ PublishRequest(_, Nil, Nil, _, _) =>
      req.complete(DiagnosticMessage.error(StatusCodes.OK, "empty payload"))
    case req @ PublishRequest(_, Nil, failures, _, _) =>
      updateStats(failures)
      val msg = FailureMessage.error(failures)
      sendError(req, StatusCodes.OK, msg)
    case req @ PublishRequest(_, values, Nil, _, _) =>
      process(values)
      req.complete(HttpResponse(StatusCodes.OK))
    case req @ PublishRequest(_, values, failures, _, _) =>
      process(values)
      updateStats(failures)
      val msg = FailureMessage.partial(failures)
      sendError(req, StatusCodes.OK, msg)
  }

  /**
    * Evaluate the datapoints against the full set of expressions and forward the results
    * to the LWC service.
    */
  private def process(values: List[Datapoint]): Unit = {
    val now = registry.clock().wallTime()
    values.foreach { v => numReceivedCounter.record(now - v.timestamp) }

    process(AllAppsGroup, values)
    values
      .filter(_.tags.contains("nf.app"))
      .groupBy(_.tags("nf.app"))
      .foreach(t => process(t._1, t._2))
  }

  /**
    * Evaluate the datapoints against the expressions in the specified group and forward
    * the intermediate aggregates, if any, to the LWC service.
    */
  private def process(group: String, values: List[Datapoint]): Unit = {
    import scala.collection.JavaConverters._

    val timestamp = fixTimestamp(values.head.timestamp)
    val payload = evaluator.eval(group, timestamp, values.map(toPair).asJava)

    if (!payload.getMetrics.isEmpty) {
      val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(payload))
      val request = HttpRequest(HttpMethods.POST, evalUri, Nil, entity)
      mkRequest("lwc-eval", request).onSuccess {
        case response => response.discardEntityBytes()
      }
    }
  }

  private def mkRequest(name: String, request: HttpRequest): Future[HttpResponse] = {
    val accessLogger = AccessLogger.newClientLogger(name, request)
    Http()(context.system).singleRequest(request).andThen { case t => accessLogger.complete(t) }
  }

  private def fixTimestamp(t: Long): Long = t / step * step

  private def toPair(d: Datapoint): TagsValuePair = {
    import scala.collection.JavaConverters._
    new TagsValuePair(d.tags.asJava, d.value)
  }

  private def sendError(req: PublishRequest, status: StatusCode, msg: FailureMessage): Unit = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    req.complete(HttpResponse(status = status, entity = entity))
  }

  private def updateStats(failures: List[ValidationResult]): Unit = {
    failures.foreach {
      case ValidationResult.Pass           => // Ignored
      case ValidationResult.Fail(error, _) =>
        registry.counter(numInvalidId.withTag("error", error)).increment()
    }
  }
}

