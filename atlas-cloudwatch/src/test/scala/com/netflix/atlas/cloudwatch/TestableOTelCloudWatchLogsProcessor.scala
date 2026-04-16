package com.netflix.atlas.cloudwatch

import com.netflix.atlas.webapi.CloudWatchLogEvent

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/**
 * Testable version of OTelCloudWatchLogsProcessor that captures new patterns instead
 * of just logging them, so tests can assert on the results.
 */
class TestableOTelCloudWatchLogsProcessor extends OTelCloudWatchLogsProcessor {

  // (group, pattern, sample)
  private val newPatternsQueue =
    new ConcurrentLinkedQueue[(String, String, String)]()

  /** Expose captured patterns for assertions. */
  def newPatterns: List[(String, String, String)] =
    newPatternsQueue.asScala.toList

  // Override the hook for new patterns — we’ll add one.
  override protected def onNewPattern(
    logGroup: String,
    logStream: String,
    subscriptionFilters: List[String],
    pattern: String,
    sample: String
  ): Unit = {
    // Call super implementation so normal logging still happens if desired
    super.onNewPattern(logGroup, logStream, subscriptionFilters, pattern, sample)
    newPatternsQueue.add((logGroup, pattern, sample))
  }
}