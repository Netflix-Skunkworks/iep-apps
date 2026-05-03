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

import com.netflix.atlas.webapi.CloudWatchLogEvent
import com.netflix.atlas.webapi.CloudWatchLogsProcessor
import com.netflix.iep.aws2.AwsClientFactory
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

class OTelCloudWatchLogsProcessor(
  config: Config,
  sink: OtelLogSink = OtelTcpSink,
  clientFactory: AwsClientFactory
) extends CloudWatchLogsProcessor
    with StrictLogging {

  // Max events per sendBatch chunk — bounds burst rate to the OTel collector.
  // Large streams are split into multiple chunks rather than truncated.
  private val maxPerBatch: Int = config.getInt("atlas.cloudwatch.logs.maxPerBatch")

  // Local in‑memory cache for log group tags
  private val tagCache = new LogGroupTagCache(clientFactory)

  override def process(
    owner: String,
    logGroup: String,
    logStream: String,
    subscriptionFilters: List[String],
    events: List[CloudWatchLogEvent]
  ): Unit = {

    logger.info(
      s"Processing ${events.length} logEvents from logGroup=$logGroup, logStream=$logStream"
    )

    // Take the first event to get account/region (all events in this batch are same account/region)
    val (accountOpt, regionOpt) =
      events.headOption.map(ev => (Option(ev.account), Option(ev.region))).getOrElse((None, None))

    // Fetch tags once per batch (cached per logGroup)
    val logGroupTags: Map[String, String] =
      (for {
        account <- accountOpt.get
        region  <- regionOpt.get
      } yield tagCache.getTags(logGroup, account, region)).getOrElse(Map.empty)

    // Build OtelLog for every event, preserving global line_index across chunks
    // so the collector can reconstruct exact order even across chunk boundaries.
    val allLogs = events.zipWithIndex.map {
      case (ev, idx) =>
        val (requestIdOpt, levelRaw, msg) = parseLogLine(ev.message)
        val level = Option(levelRaw).filter(_.nonEmpty).getOrElse("INFO").toUpperCase

        val baseTags: Map[String, Any] = Map(
          "nf.app"         -> "ObservabilityLogsExporter",
          "aws_request_id" -> requestIdOpt.orNull,
          "aws_account"    -> ev.account.orNull,
          "aws_region"     -> ev.region.orNull,
          "aws_log_group"  -> logGroup,
          "aws_log_stream" -> logStream,
          "cw_event_id"    -> ev.id,
          "line_index"     -> idx,
          "source"         -> subscriptionFilters.mkString("/")
        )

        // logGroupTags values win for duplicate keys
        val mergedTags: Map[String, Any] = baseTags ++ logGroupTags

        OtelTcpLogger.buildLog(
          message = msg,
          level = level,
          loggerName = "cwlogs.subscription",
          tags = mergedTags
        )
    }

    // Split into bounded chunks so each sendBatch call delivers at most maxPerBatch
    // events to the OTel collector — prevents a single large stream from causing
    // a burst of TCP sends that could OOM the collector.
    allLogs.grouped(maxPerBatch).foreach(sink.sendBatch)
  }

  /**
   * Generic log line parser:
   * - If it's Lambda-style "\t" format, extract requestId/level/message.
   * - Otherwise default to whole line as message with INFO level.
   */
  private def parseLogLine(raw: String): (Option[String], String, String) = {
    if (raw == null) return (None, "INFO", "")

    val line = raw.stripLineEnd

    // Try Lambda-style: 2026-04-11T06:52:17.826Z \t <requestId> \t INFO \t message
    val tabParts = line.split("\t", 4)
    if (tabParts.length >= 4) {
      val requestId = Option(tabParts(1))
      val level = Option(tabParts(2)).map(_.toUpperCase).getOrElse("INFO")
      val msg = tabParts.drop(3).mkString("\t")
      (requestId, level, msg)
    } else {
      // Not Lambda-style; treat entire line as message
      (None, "INFO", line)
    }
  }
}
