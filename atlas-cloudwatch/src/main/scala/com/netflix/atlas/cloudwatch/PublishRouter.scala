/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import akka.actor.ActorSystem
import com.netflix.atlas.akka.AkkaHttpClient
import com.netflix.atlas.cloudwatch.PublishRouter.defaultKey
import com.netflix.iep.config.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.Executors
import scala.jdk.CollectionConverters.CollectionHasAsScala

class PublishRouter(
  config: Config,
  registry: Registry,
  tagger: Tagger,
  httpClient: AkkaHttpClient
)(implicit system: ActorSystem)
    extends StrictLogging {

  private val schedulers = Executors.newScheduledThreadPool(2)
  private val baseURI = config.getString("atlas.cloudwatch.account.routing.uri")

  private val missingAccount =
    registry.counter("atlas.cloudwatch.queue.dps.dropped", "reason", "missingAccount")

  private[cloudwatch] val mainQueue = new PublishQueue(
    config.getConfig("atlas.cloudwatch.account.routing"),
    registry,
    "main",
    baseURI
      .replaceAll("\\$\\{STACK\\}", "main")
      .replaceAll("\\$\\{REGION}", NetflixEnvironment.region()),
    httpClient,
    schedulers
  )

  //                                      acct,       region, queue
  private[cloudwatch] val accountMap: Map[String, Map[String, PublishQueue]] = {
    var accounts = Map.empty[String, Map[String, PublishQueue]]
    config
      .getConfigList("atlas.cloudwatch.account.routing.routes")
      .asScala
      .foreach { cfg =>
        val stack = cfg.getString("stack")
        cfg
          .getConfigList("accounts")
          .asScala
          .foreach { c =>
            val account = c.getString("account")
            if (accounts.contains(account)) {
              throw new IllegalArgumentException(
                s"Account ${account} can only appear once in the config."
              )
            }
            //                     region, queue
            var routes = Map.empty[String, PublishQueue]
            if (c.hasPath("routing")) {
              routes = c
                .getConfig("routing")
                .entrySet()
                .asScala
                .map { r =>
                  val destination = r.getValue.unwrapped().toString
                  r.getKey -> new PublishQueue(
                    config.getConfig("atlas.cloudwatch.account.routing"),
                    registry,
                    destination,
                    baseURI
                      .replaceAll("\\$\\{STACK\\}", stack)
                      .replaceAll("\\$\\{REGION}", destination),
                    httpClient,
                    schedulers
                  )
                }
                .toMap
            }

            routes += defaultKey -> new PublishQueue(
              config.getConfig("atlas.cloudwatch.account.routing"),
              registry,
              NetflixEnvironment.region(),
              baseURI
                .replaceAll("\\$\\{STACK\\}", stack)
                .replaceAll("\\$\\{REGION}", NetflixEnvironment.region()),
              httpClient,
              schedulers
            )

            accounts += account -> routes
          }
      }
    accounts
  }
  logger.info(s"Loaded ${accountMap.size} accounts plus main.")

  /**
    * Routes the data to the proper queue based on the `nf.account` tag.
    *
    * @param datapoint
    *     The non-null data point.
    */
  def publish(datapoint: AtlasDatapoint): Unit = {
    val formatted = tagger.fixTags(datapoint)
    getQueue(formatted) match {
      case Some(queue) => queue.enqueue(formatted)
      case None        => missingAccount.increment()
    }
  }

  private[cloudwatch] def getQueue(datapoint: AtlasDatapoint): Option[PublishQueue] = {
    datapoint.tags.get("nf.account") match {
      case Some(account) =>
        accountMap.get(account) match {
          case Some(regionMap) =>
            val region = datapoint.tags.get("nf.region").getOrElse(defaultKey)
            regionMap.get(region) match {
              case Some(queue) => Some(queue)
              case None        => regionMap.get(defaultKey)
            }
          case None => Some(mainQueue)
        }
      case None => None
    }
  }

  def shutdown: Unit = {
    schedulers.shutdownNow()
  }
}

object PublishRouter {
  private[cloudwatch] val defaultKey = "_DEFAULT"
}
