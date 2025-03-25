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
package com.netflix.atlas.cloudwatch

import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.cloudwatch.PublishRouter.defaultKey
import com.netflix.atlas.cloudwatch.poller.PublishClient
import com.netflix.atlas.cloudwatch.poller.PublishConfig
import com.netflix.iep.config.NetflixEnvironment
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.Executors
import scala.jdk.CollectionConverters.CollectionHasAsScala

class PublishRouter(
  config: Config,
  registry: Registry,
  tagger: Tagger,
  httpClient: PekkoHttpClient,
  val status: LeaderStatus
)(implicit system: ActorSystem)
    extends StrictLogging {

  private val schedulers = Executors.newScheduledThreadPool(2)
  private val baseURI = config.getString("atlas.cloudwatch.account.routing.uri")
  private val baseConfigURI = config.getString("atlas.cloudwatch.account.routing.config-uri")
  private val baseEvalURI = config.getString("atlas.cloudwatch.account.routing.eval-uri")

  private val missingAccount =
    registry.counter("atlas.cloudwatch.queue.dps.dropped", "reason", "missingAccount")

  private[cloudwatch] val mainQueue = buildPubQueue(config, "main", NetflixEnvironment.region())

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
                  r.getKey -> buildPubQueue(config, stack, destination)
                }
                .toMap
            }

            // Skip the _DEFAULT queue, if current region entry already present in "routing"
            if (routes.contains(NetflixEnvironment.region())) {
              routes += (defaultKey -> routes.getOrElse(
                NetflixEnvironment.region(),
                throw new NoSuchElementException(
                  s"Region ${NetflixEnvironment.region()} not found in routes"
                )
              ))
            } else {
              routes += defaultKey -> buildPubQueue(
                config,
                stack,
                NetflixEnvironment.region()
              )
            }

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

  /**
   * Routes the data to the proper atlas registry instance based on the `nf.account` and region tag.
   *
   * @param datapoint
   *     The non-null data point.
   */
  def publishToRegistry(datapoint: AtlasDatapoint, cwDataPoint: CloudWatchDatapoint): Unit = {
    getQueue(datapoint) match {
      case Some(queue) => queue.updateRegistry(datapoint, cwDataPoint)
      case None        => missingAccount.increment()
    }
  }

  private[cloudwatch] def getQueue(tags: Map[String, String]): Option[PublishQueue] = {
    tags.get("nf.account") match {
      case Some(account) =>
        accountMap.get(account) match {
          case Some(regionMap) =>
            val region = tags.get("nf.region").getOrElse(defaultKey)
            regionMap.get(region) match {
              case Some(queue) => Some(queue)
              case None        => regionMap.get(defaultKey)
            }
          case None => Some(mainQueue)
        }
      case None => None
    }
  }

  private[cloudwatch] def getQueue(datapoint: AtlasDatapoint): Option[PublishQueue] = {
    getQueue(datapoint.tags)
  }

  private[cloudwatch] def buildPubQueue(
    config: Config,
    stack: String,
    destination: String
  ): PublishQueue = {
    val cfg = config.getConfig("atlas.cloudwatch.account.routing")
    val pubConfig = new PublishConfig(
      cfg,
      baseURI
        .replaceAll("\\$\\{STACK\\}", stack)
        .replaceAll("\\$\\{REGION\\}", destination),
      baseConfigURI
        .replaceAll("\\$\\{STACK\\}", stack)
        .replaceAll("\\$\\{REGION\\}", destination),
      baseEvalURI
        .replaceAll("\\$\\{STACK\\}", stack)
        .replaceAll("\\$\\{REGION\\}", destination),
      status,
      registry
    )
    val streamingRegistryClient = new PublishClient(pubConfig)

    logger.info(
      s"Setup queue for stack ${stack} publishing URI ${pubConfig.uri}, " +
        s"lwc-config URI ${pubConfig.configUri}, eval URI ${pubConfig.evalUri}"
    )
    new PublishQueue(
      cfg,
      registry,
      stack + "-" + destination,
      status,
      streamingRegistryClient,
      httpClient,
      schedulers
    )
  }

  def shutdown(): Unit = {
    schedulers.shutdownNow()
  }
}

object PublishRouter {
  private[cloudwatch] val defaultKey = "_DEFAULT"
}
