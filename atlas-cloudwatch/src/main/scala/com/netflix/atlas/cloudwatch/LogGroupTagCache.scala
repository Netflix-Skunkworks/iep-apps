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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/**
 * Simple in‑memory cache for CloudWatch Logs group tags.
 *
 * Cache key: log group name (String)
 * Value: Map[tagKey -> tagValue]
 *
 * ListTagsForResource is called the first time for a given log group, and then
 * again every refreshInterval to pick up tag changes made on the AWS side.
 */
class LogGroupTagCache(
  clientFactory: AwsClientFactory,
  refreshInterval: FiniteDuration = 15.minutes
) extends StrictLogging {

  // logGroupName -> Map[tagKey -> tagValue]
  private val cache = new ConcurrentHashMap[String, Map[String, String]]()

  // logGroupName -> (accountId, region), needed to re-fetch tags on refresh
  private val lookupKeys = new ConcurrentHashMap[String, (String, String)]()

  private val refreshExecutor =
    Executors.newSingleThreadScheduledExecutor(runnable => {
      val t = new Thread(runnable, "log-group-tag-cache-refresh")
      t.setDaemon(true)
      t
    })

  refreshExecutor.scheduleAtFixedRate(
    () => refreshAll(),
    refreshInterval.toMillis,
    refreshInterval.toMillis,
    TimeUnit.MILLISECONDS
  )

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

    lookupKeys.put(logGroupName, (accountId, region))
    val tags = fetchTags(logGroupName, accountId, region)
    cache.put(logGroupName, tags)
    tags
  }

  private def fetchTags(
    logGroupName: String,
    accountId: String,
    region: String
  ): Map[String, String] = {
    val arn = s"arn:aws:logs:$region:$accountId:log-group:$logGroupName"

    try {
      val req = ListTagsForResourceRequest.builder().resourceArn(arn).build()
      val resp = createClient(accountId, Region.of(region)).listTagsForResource(req)
      val tags = Option(resp.tags())
        .map(_.asScala.toMap)
        .getOrElse(Map.empty[String, String])

      logger.info(s"Fetched tags for log group $logGroupName (arn=$arn): $tags")
      tags
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to list tags for log group $logGroupName (arn=$arn); returning empty tag map",
          e
        )
        Map.empty[String, String]
    }
  }

  private def refreshAll(): Unit = {
    lookupKeys.forEach { (logGroupName, key) =>
      val (accountId, region) = key
      val tags = fetchTags(logGroupName, accountId, region)
      val previous = cache.put(logGroupName, tags)
      if (previous != null && previous != tags) {
        logger.info(s"Tags updated on refresh for log group $logGroupName: $previous -> $tags")
      }
    }
  }
}
