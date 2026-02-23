/*
 * Copyright 2014-2026 Netflix, Inc.
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

  // Root routing config
  private val routingCfg = config.getConfig("atlas.cloudwatch.account.routing")
  private val baseURI = routingCfg.getString("uri")
  private val baseConfigURI = routingCfg.getString("config-uri")
  private val baseEvalURI = routingCfg.getString("eval-uri")

  private val missingAccount =
    registry.counter("atlas.cloudwatch.queue.dps.dropped", "reason", "missingAccount")

  // mainQueue is not tied to a specific route, so we pass routingCfg as the "route" config
  private[cloudwatch] val mainQueue =
    buildPubQueue(config, routingCfg, "main", NetflixEnvironment.region())

  //                                      acct,       region, queue
  private[cloudwatch] val accountMap: Map[String, Map[String, PublishQueue]] = {
    var accounts = Map.empty[String, Map[String, PublishQueue]]

    config
      .getConfigList("atlas.cloudwatch.account.routing.routes")
      .asScala
      .foreach { routeCfg =>
        val stack = routeCfg.getString("stack")
        routeCfg
          .getConfigList("accounts")
          .asScala
          .foreach { c =>
            val account = c.getString("account")
            if (accounts.contains(account)) {
              throw new IllegalArgumentException(
                s"Account $account can only appear once in the config."
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
                  // NOTE: pass routeCfg so buildPubQueue can see dual-registry flags
                  r.getKey -> buildPubQueue(config, routeCfg, stack, destination)
                }
                .toMap
            }

            // Skip the _DEFAULT queue if current region entry already present in "routing"
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
                routeCfg,
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

  /**
   * Build a PublishQueue for a given stack+destination.
   *
   * @param config    full app config
   * @param routeCfg  config block for the specific route (one element from routes[])
   * @param stack     logical stack for this route (e.g., "stackA")
   * @param destination region / destination (e.g., "us-west-1")
   */
  private[cloudwatch] def buildPubQueue(
    config: Config,
    routeCfg: Config,
    stack: String,
    destination: String
  ): PublishQueue = {

    // Whether this route wants dual-registry
    val dualRegistryEnabled =
      routeCfg.hasPath("dual-registry") && routeCfg.getBoolean("dual-registry")

    // Secondary stack to use when dual-registry is enabled (e.g., "stackC")
    val dualRegistryStackOpt: Option[String] =
      if (dualRegistryEnabled && routeCfg.hasPath("dual-registry-stack"))
        Some(routeCfg.getString("dual-registry-stack"))
      else
        None

    val cfg = config.getConfig("atlas.cloudwatch.account.routing")

    // Primary publish config (STACK = stack, e.g., "stackA")
    val primaryPubConfig = new PublishConfig(
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
    val primaryClient = new PublishClient(primaryPubConfig)

    // Optional secondary publish config (STACK = dual-registry-stack, e.g., "stackC")
    val secondaryClientOpt: Option[PublishClient] =
      dualRegistryStackOpt.map { dualStack =>
        val secondaryPubConfig = new PublishConfig(
          cfg,
          baseURI
            .replaceAll("\\$\\{STACK\\}", dualStack)
            .replaceAll("\\$\\{REGION\\}", destination),
          baseConfigURI
            .replaceAll("\\$\\{STACK\\}", dualStack)
            .replaceAll("\\$\\{REGION\\}", destination),
          baseEvalURI
            .replaceAll("\\$\\{STACK\\}", dualStack)
            .replaceAll("\\$\\{REGION\\}", destination),
          status,
          registry
        )
        new PublishClient(secondaryPubConfig)
      }

    if (dualRegistryEnabled && secondaryClientOpt.isEmpty) {
      logger.warn(
        s"dual-registry=true for stack=$stack but dual-registry-stack is not set or invalid. " +
          "Proceeding with primary registry only."
      )
    }

    logger.info(
      s"Setup queue for stack=$stack destination=$destination " +
        s"primary URI=${primaryPubConfig.uri}, lwc-config URI=${primaryPubConfig.configUri}, " +
        s"eval URI=${primaryPubConfig.evalUri}, " +
        s"dualRegistryEnabled=$dualRegistryEnabled secondaryConfigured=${secondaryClientOpt.isDefined}"
    )

    secondaryClientOpt.foreach { c =>
      logger.info(
        s"Secondary registry for stack=$stack destination=$destination " +
          s"URI=${c.config.uri}, lwc-config URI=${c.config.configUri}, eval URI=${c.config.evalUri}"
      )
    }

    new PublishQueue(
      cfg,
      registry,
      stack + "-" + destination,
      status,
      primaryClient,
      secondaryClientOpt,
      dualRegistryEnabled = dualRegistryEnabled && secondaryClientOpt.isDefined,
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
