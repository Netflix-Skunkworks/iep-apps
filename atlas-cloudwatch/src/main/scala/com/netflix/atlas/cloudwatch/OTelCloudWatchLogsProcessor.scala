package com.netflix.atlas.cloudwatch

import com.netflix.atlas.webapi.{CloudWatchLogEvent, CloudWatchLogsProcessor}
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

class OTelCloudWatchLogsProcessor extends CloudWatchLogsProcessor with StrictLogging {

  private val seenPatterns =
    new ConcurrentHashMap[String, java.lang.Boolean]().asScala

  override def process(
    owner: String,
    logGroup: String,
    logStream: String,
    subscriptionFilters: List[String],
    events: List[CloudWatchLogEvent]
  ): Unit = {

    events.foreach { ev =>
      val (requestIdOpt, level, msg) = parseLogLine(ev.message)

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
      
      // TODO: send an OTel log record if it passes the log rule criteria
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

  /**
   * Derive a "pattern" from a raw message string by normalizing variable parts.
   * This is heuristic and can be tuned for your data.
   */
  private def derivePattern(msg: String): String = {
    if (msg == null || msg.isEmpty) return "<EMPTY>"

    val trimmed = msg.trim

    // If it looks like JSON, keep it but normalize numbers/ids inside
    val isJsonLike =
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]"))

    if (isJsonLike) {
      normalizeJsonish(trimmed)
    } else {
      normalizeText(trimmed)
    }
  }

  /**
   * Basic normalization appropriate for text logs:
   * - Replace ISO timestamps with <TS>
   * - Replace nginx-style timestamps with <TS>
   * - Replace UUID-like strings with <UUID>
   * - Replace large numbers and PIDs with <NUM>
   * - Collapse whitespace
   *
   * Example:
   *   "2026/04/16 02:44:34 [notice] 105#105: start worker process 117"
   *   -> "<TS> [notice] <NUM>#<NUM>: start worker process <NUM>"
   */
  private def normalizeText(s: String): String = {
    var t = s

    // ISO8601 timestamp: 2026-04-16T02:44:34.988Z
    t = t.replaceAll(
      raw"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z",
      "<TS>"
    )

    // ISO8601 with space and comma: 2026-04-16 02:44:34,988
    t = t.replaceAll(
      raw"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?",
      "<TS>"
    )

    // nginx-style timestamp: 2026/04/16 02:44:34
    t = t.replaceAll(
      raw"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}",
      "<TS>"
    )

    // UUID
    t = t.replaceAll(
      raw"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
      "<UUID>"
    )

    // IP addresses
    t = t.replaceAll(
      raw"\b\d{1,3}(?:\.\d{1,3}){3}\b",
      "<IP>"
    )

    // Numeric IDs / PIDs / ports etc. (4+ digits)
    t = t.replaceAll(
      raw"\b\d{4,}\b",
      "<NUM>"
    )

    // Collapse repeated spaces
    t = t.replaceAll(raw"\s+", " ").trim

    t
  }

  /**
   * For JSON-ish logs we can still use text normalization.
   * If you later want to parse JSON properly and normalize field-by-field,
   * you can plug that in here.
   */
  private def normalizeJsonish(s: String): String = {
    // For now just reuse text normalization; it will replace numbers, UUIDs, etc.
    normalizeText(s)
  }
}