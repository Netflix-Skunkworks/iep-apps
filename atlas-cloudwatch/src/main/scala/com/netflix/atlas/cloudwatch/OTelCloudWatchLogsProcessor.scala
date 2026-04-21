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
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

class OTelCloudWatchLogsProcessor(
  sink: OtelLogSink = OtelTcpSink
) extends CloudWatchLogsProcessor
    with StrictLogging {

  private val seenPatterns =
    new ConcurrentHashMap[String, java.lang.Boolean]().asScala

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

    events.zipWithIndex.foreach {
      case (ev, idx) =>
        val (requestIdOpt, levelRaw, msg) = parseLogLine(ev.message)
        val level = Option(levelRaw).filter(_.nonEmpty).getOrElse("INFO").toUpperCase

        // Derive a normalized "pattern" from the message text
        val pattern = derivePattern(msg)

        // Scope uniqueness by logGroup + pattern
        val key = s"$logGroup::$pattern"

        // Only log when we see a new pattern for the first time
        val isNew = seenPatterns.putIfAbsent(key, java.lang.Boolean.TRUE).isEmpty
        if (isNew) {
          onNewPattern(
            logGroup = logGroup,
            logStream = logStream,
            subscriptionFilters = subscriptionFilters,
            pattern = pattern,
            sample = msg
          )
        }

        val tags: Map[String, Any] = Map(
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

        val logDoc = OtelTcpLogger.buildLog(
          message = msg,
          level = level,
          loggerName = "cwlogs.subscription",
          tags = tags
        )

        sink.send(logDoc)
    }
  }

  /** Hook for subclasses/tests to intercept new patterns. */
  protected def onNewPattern(
    logGroup: String,
    logStream: String,
    subscriptionFilters: List[String],
    pattern: String,
    sample: String
  ): Unit = {
    logger.info(
      s"NEW_LOG_PATTERN group=$logGroup stream=$logStream " +
        s"filters=${subscriptionFilters.mkString(",")} " +
        s"pattern=$pattern sample=${sample.take(500)}"
    )
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

  private def derivePattern(msg: String): String = {
    if (msg == null || msg.isEmpty) return "<EMPTY>"

    val trimmed = msg.trim

    val isJsonLike =
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))

    if (isJsonLike) {
      normalizeJsonish(trimmed)
    } else {
      normalizeText(trimmed)
    }
  }

  private def normalizeText(s: String): String = {
    var t = s

    t = t.replaceAll(
      raw"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z",
      "<TS>"
    )
    t = t.replaceAll(
      raw"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?",
      "<TS>"
    )
    t = t.replaceAll(
      raw"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}",
      "<TS>"
    )
    t = t.replaceAll(
      raw"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
      "<UUID>"
    )
    t = t.replaceAll(
      raw"\b\d{1,3}(?:\.\d{1,3}){3}\b",
      "<IP>"
    )
    t = t.replaceAll(
      raw"\b\d{4,}\b",
      "<NUM>"
    )
    t = t.replaceAll(raw"\s+", " ").trim

    t
  }

  private def normalizeJsonish(s: String): String = {
    normalizeText(s)
  }
}
