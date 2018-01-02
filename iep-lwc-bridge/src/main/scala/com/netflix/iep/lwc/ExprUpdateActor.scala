/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.json.Json
import com.netflix.frigga.Names
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.impl.Evaluator
import com.netflix.spectator.atlas.impl.Query
import com.netflix.spectator.atlas.impl.Subscription
import com.netflix.spectator.atlas.impl.Subscriptions
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future


/**
  * Refresh the set of expressions from the LWC service.
  */
class ExprUpdateActor(config: Config, registry: Registry, evaluator: Evaluator)
  extends Actor with StrictLogging {

  import ExprUpdateActor._
  import scala.concurrent.duration._

  import scala.concurrent.ExecutionContext.Implicits.global

  type SubscriptionList = java.util.List[Subscription]

  private implicit val materializer = ActorMaterializer()

  private val configUri = Uri(config.getString("netflix.iep.lwc.bridge.config-uri"))

  private val exprsPerApp = registry.distributionSummary("lwc.bridge.exprsPerApp")
  private val fallbackExprs = registry.distributionSummary("lwc.bridge.fallbackExprs")

  private val cancellable = context.system.scheduler.schedule(0.seconds, 10.seconds, self, Tick)

  def receive: Receive = {
    case Tick => updateExpressions()
  }

  override def postStop(): Unit = {
    cancellable.cancel()
    super.postStop()
  }

  private def updateExpressions(): Unit = {
    val request = HttpRequest(HttpMethods.GET, configUri)
    mkRequest("lwc-subs", request).foreach {
      case response if response.status == StatusCodes.OK =>
        response.entity.dataBytes.runReduce(_ ++ _).foreach {
          case data =>
            val exprs = Json.decode[Subscriptions](data.toArray).getExpressions
            updateEvaluator(exprs)
        }
      case response =>
        response.discardEntityBytes()
    }
  }

  /**
    * Create groups of expressions per application. If a query does not have an exact
    * match to a single application, then put it in the `-all-` group that will be
    * evaluated for every datapoint.
    */
  private def updateEvaluator(exprs: SubscriptionList): Unit = {
    val appGroups = new java.util.HashMap[String, SubscriptionList]()
    val iter = exprs.iterator()
    while (iter.hasNext) {
      val sub = iter.next()
      val app = getApp(sub.dataExpr().query())
      val subs = appGroups.computeIfAbsent(app, _ => new java.util.ArrayList[Subscription]())
      subs.add(sub)
    }
    appGroups.forEach { (app, subs) =>
      if (app == AllAppsGroup)
        fallbackExprs.record(subs.size())
      else
        exprsPerApp.record(subs.size())
    }
    evaluator.sync(appGroups)
  }

  /**
    * Extract an exact app name for the query if possible. This is used as a quick first
    * level filter to reduce the number of expressions that must be checked per datapoint.
    * If `nf.app` is not present, then it will attempt to use frigga to extract an app name
    * from `nf.cluster` or `nf.asg`.
    */
  private def getApp(query: Query): String = {
    try {
      val exactTags = query.exactTags()
      List("nf.app", "nf.cluster", "nf.asg").find(exactTags.containsKey) match {
        case Some(k) =>
          val name = Names.parseName(exactTags.get(k))
          Option(name.getApp).getOrElse(AllAppsGroup)
        case None =>
          AllAppsGroup
      }
    } catch {
      case e: Exception =>
        logger.debug(s"failed to extract app name from: $query", e)
        AllAppsGroup
    }
  }

  private def mkRequest(name: String, request: HttpRequest): Future[HttpResponse] = {
    val accessLogger = AccessLogger.newClientLogger(name, request)
    Http()(context.system).singleRequest(request).andThen { case t => accessLogger.complete(t) }
  }
}

object ExprUpdateActor {
  case object Tick
  private[lwc] val AllAppsGroup = "-all-"
}

