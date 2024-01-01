/*
 * Copyright 2014-2024 Netflix, Inc.
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
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonParserHelper.foreachField
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.config.ConfigClient
import software.amazon.awssdk.services.config.model.SelectAggregateResourceConfigRequest

import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.compat.java8.StreamConverters.StreamHasToScala
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Random
import scala.util.Using

/**
  * Periodically calls configuration aggregators for a list of accounts and active
  * resources in each region. The list is filtered through the regions we want in the config
  * and accounts can be allowed or denied via a list and "is-allow" flag.
  *
  * Note that on startup, the config is loaded and exceptions thrown so that we don't start
  * an instance that isn't ready to handle data.
  */
class AwsConfigAccountSupplier(
  config: Config,
  registry: Registry,
  clientFactory: AwsClientFactory
)(implicit val system: ActorSystem)
    extends AwsAccountSupplier
    with StrictLogging {

  private val aggregator = config.getString("atlas.cloudwatch.account.supplier.aws.aggregator")

  private val aggregatorRegion =
    Region.of(config.getString("atlas.cloudwatch.account.supplier.aws.aggregator-region"))

  private val configAccount =
    config.getString("atlas.cloudwatch.account.supplier.aws.config-account")

  private val regions =
    config.getStringList("atlas.cloudwatch.account.supplier.aws.regions").asScala.toSet
  private val isAllow = config.getBoolean("atlas.cloudwatch.account.supplier.aws.is-allow")

  private val accountList =
    config.getStringList("atlas.cloudwatch.account.supplier.aws.accounts").asScala.toSet
  private val initialized = new AtomicBoolean()

  @volatile private[cloudwatch] var rawAccountResources
    : Map[String, Map[Region, Map[String, Set[String]]]] = null
  @volatile private[cloudwatch] var filtered: Map[String, Map[Region, Set[String]]] = null

  private val runner: Runnable = () => {
    val start = System.currentTimeMillis()
    logger.info(
      s"Starting load of AWS accounts and resources from AWS Config aggregator ${aggregator} in ${aggregatorRegion}"
    )
    //                  acct        region      ns      resources
    var map = Map.empty[String, Map[Region, Map[String, Set[String]]]]

    try {
      val client: ConfigClient = clientFactory.getInstance(
        "config-query",
        classOf[ConfigClient],
        configAccount,
        Optional.of(aggregatorRegion)
      )
      val req = SelectAggregateResourceConfigRequest
        .builder()
        .configurationAggregatorName(aggregator)
        .expression(
          "SELECT COUNT(*), accountId, resourceType, awsRegion GROUP BY accountId, resourceType, awsRegion"
        )
        .build()

      val resp = client.selectAggregateResourceConfigPaginator(req)
      var records = 0
      var skipped = 0
      // Should be fine for a smallish # off records. If it becomes a problem, stream.
      resp.results().stream().toScala(List).foreach { json =>
        try {
          Using.resource(Json.newJsonParser(json)) { parser =>
            var account: String = null
            var resource: String = null
            var region: String = null
            foreachField(parser) {
              case "accountId"    => account = parser.nextTextValue()
              case "resourceType" => resource = parser.nextTextValue()
              case "awsRegion"    => region = parser.nextTextValue()
              case _ =>
                parser.nextToken()
                parser.skipChildren()
            }
            records += 1

            if (regions.contains(region)) {
              val r = region match {
                case "global" => Region.AWS_GLOBAL
                case other    => Region.of(other)
              }
              var regions = map.getOrElse(account, Map.empty)
              var nss = regions.getOrElse(r, Map.empty)
              val (ns, remainder) = splitResource(resource)
              var set = nss.getOrElse(ns, Set.empty)
              set += remainder
              nss += ns      -> set
              regions += r   -> nss
              map += account -> regions
            } else {
              skipped += 1
            }
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to parse resource: ${json}", ex)
            registry
              .counter(
                "atlas.cloudwatch.account.supplier.aws.parseException",
                "exception",
                ex.getClass.getSimpleName
              )
              .increment()
        }
      }
      logger.info(s"Successfully loaded ${records} records (skipped ${skipped})")
    } catch {
      case ex: Exception =>
        logger.error("Failed to query for resources", ex)
        registry
          .counter(
            "atlas.cloudwatch.account.supplier.aws.loadException",
            "exception",
            ex.getClass.getSimpleName
          )
          .increment()
        throw ex
    }
    rawAccountResources = map
    val f = if (isAllow) {
      rawAccountResources.filter { case (account, _) => accountList.contains(account) }
    } else {
      rawAccountResources.filterNot { case (account, _) => accountList.contains(account) }
    }
    filtered = f.map(acct => acct._1 -> acct._2.map(r => r._1 -> r._2.keySet))
    logger.info(
      s"Finished loading ${rawAccountResources.size} (${filtered.size} filtered) AWS accounts and resources in ${(System.currentTimeMillis() - start) / 1000.0} seconds"
    )
    logger.info(s"Final AWS accounts: ${filtered}")
    registry
      .timer("atlas.cloudwatch.account.supplier.aws.loadTime")
      .record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
    initialized.set(true)
  }
  // run now to make sure we have access and can start
  runner.run()

  private val delay = {
    val now = System.currentTimeMillis()
    val normalized = normalize(now, 86_400) + (new Random().nextInt(3600) * 1000)
    (if (now < normalized) normalized - now else (normalized + (86400 * 1000)) - now) / 1000
  }
  system.scheduler.scheduleAtFixedRate(delay.seconds, 24.hours)(runner)(system.dispatcher)
  logger.info(s"Next AWS Config load in ${delay} seconds.")

  override def accounts: Map[String, Map[Region, Set[String]]] = filtered

  private def splitResource(resource: String): (String, String) = {
    var idx = resource.indexOf("::")
    if (idx < 0) throw new IllegalArgumentException(s"Invalid resource: ${resource}")
    idx = resource.indexOf("::", idx + 2)
    if (idx < 0) throw new IllegalArgumentException(s"Invalid resource: ${resource}")
    val namespace = resource.substring(0, idx).replaceAll("::", "/")
    val remainder = resource.substring(idx + 2).replaceAll("::", "/")
    (namespace, remainder)
  }
}
