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

import com.netflix.iep.aws2.AwsClientFactory
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model.ListTagsForResourceRequest

import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
 * Simple in‑memory cache for CloudWatch Logs group tags.
 *
 * Cache key: log group name (String)
 * Value: Map[tagKey -> tagValue]
 *
 * It only calls ListTagsForResource the first time for a given log group.
 */
class LogGroupTagCache(clientFactory: AwsClientFactory) extends StrictLogging {

  // logGroupName -> Map[tagKey -> tagValue]
  private val cache = new ConcurrentHashMap[String, Map[String, String]]()

  private def createClient(account: String, region: Region): CloudWatchLogsClient = {
    clientFactory.getInstance(
      s"$account.$region",
      classOf[CloudWatchLogsClient],
      account,
      Optional.of(region)
    )
  }

  def getTags(logGroupName: String, accountId: String, region: String): Map[String, String] = {
    // Fast path: return from cache if present (even if empty map)
    val cached = cache.get(logGroupName)
    if (cached != null) return cached

    // Not cached: fetch from AWS
    val arn =
      s"arn:aws:logs:$region:$accountId:log-group:$logGroupName"

    try {
      val req = ListTagsForResourceRequest.builder().resourceArn(arn).build()
      val resp = createClient(accountId, Region.of(region)).listTagsForResource(req)
      val tags = Option(resp.tags())
        .map(_.asScala.toMap)
        .getOrElse(Map.empty[String, String])

      cache.put(logGroupName, tags)
      tags
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to list tags for log group $logGroupName (arn=$arn); returning empty tag map",
          e
        )
        // Cache the empty map to avoid repeated failing calls
        val empty = Map.empty[String, String]
        cache.put(logGroupName, empty)
        empty
    }
  }
}
