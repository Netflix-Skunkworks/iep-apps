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

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Simple Typesafe config based AWS account supplier. Used for testing and in place of a supplier using internal tooling.
  */
class ConfigAccountSupplier(
  config: Config,
  rules: CloudWatchRules
) extends AwsAccountSupplier
    with StrictLogging {

  private[cloudwatch] val namespaces = rules.rules.keySet

  private[cloudwatch] val defaultRegions =
    if (config.hasPath("atlas.cloudwatch.account.polling.default-regions"))
      config
        .getStringList("atlas.cloudwatch.account.polling.default-regions")
        .asScala
        .map(Region.of)
        .toList
    else List(Region.of(System.getenv("NETFLIX_REGION")))

  private val map = config
    .getConfigList("atlas.cloudwatch.account.polling.accounts")
    .asScala
    .map { c =>
      val regions = if (c.hasPath("regions")) {
        c.getStringList("regions").asScala.map(Region.of(_) -> namespaces).toMap
      } else {
        defaultRegions.map(_ -> namespaces).toMap
      }
      c.getString("account") -> regions
    }
    .toMap
  logger.debug(s"Loaded accounts: ${map}")

  /**
    * @return The non-null list of account IDs to poll for CloudWatch metrics.
    */
  override val accounts: Map[String, Map[Region, Set[String]]] = map
}
