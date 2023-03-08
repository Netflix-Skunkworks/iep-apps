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
    baseURI.replaceAll("\\$\\{STACK\\}", "main"),
    httpClient,
    schedulers
  )

  private[cloudwatch] val accountMap = config
    .getConfig("atlas.cloudwatch.account.routing.stackMap")
    .entrySet()
    .asScala
    .flatMap { e =>
      e.getValue.unwrapped().asInstanceOf[java.util.List[String]].asScala.map { acct =>
        val queue = new PublishQueue(
          config.getConfig("atlas.cloudwatch.account.routing"),
          registry,
          e.getKey,
          baseURI.replaceAll("\\$\\{STACK\\}", e.getKey),
          httpClient,
          schedulers
        )
        acct -> queue
      }
    }
    .toMap
  logger.info(s"Loaded ${accountMap.size} accounts plus main.")

  /**
    * Routes the data to the proper queue based on the `nf.account` tag.
    *
    * @param datapoint
    *     The non-null data point.
    */
  def publish(datapoint: AtlasDatapoint): Unit = {
    val formatted = tagger.fixTags(datapoint)
    formatted.tags.get("nf.account") match {
      case Some(account) =>
        accountMap.get(account) match {
          case Some(queue) => queue.enqueue(formatted)
          case None        => mainQueue.enqueue(formatted)
        }
      case None =>
        missingAccount.increment()
    }
  }

  def shutdown(): Unit = {
    schedulers.shutdownNow()
  }
}
