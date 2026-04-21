package com.netflix.atlas.cloudwatch

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/**
 * Testable version of OTelCloudWatchLogsProcessor that:
 *  - captures new patterns, and
 *  - uses a stub OTEL sink to capture sent logs.
 */
class TestableOTelCloudWatchLogsProcessor(
  val sinkStub: StubOtelLogSink = new StubOtelLogSink
) extends OTelCloudWatchLogsProcessor(sinkStub) {

  // (group, pattern, sample)
  private val newPatternsQueue =
    new ConcurrentLinkedQueue[(String, String, String)]()

  /** Expose captured patterns for assertions. */
  def newPatterns: List[(String, String, String)] =
    newPatternsQueue.asScala.toList

  /** Expose captured OTEL logs for assertions. */
  def sentLogs: List[OtelLog] =
    sinkStub.logs

  override protected def onNewPattern(
    logGroup: String,
    logStream: String,
    subscriptionFilters: List[String],
    pattern: String,
    sample: String
  ): Unit = {
    super.onNewPattern(logGroup, logStream, subscriptionFilters, pattern, sample)
    newPatternsQueue.add((logGroup, pattern, sample))
  }
}

class StubOtelLogSink extends OtelLogSink {

  private val queue = new ConcurrentLinkedQueue[OtelLog]()

  override def send(log: OtelLog): Unit = {
    queue.add(log)
  }

  def logs: List[OtelLog] =
    queue.asScala.toList
}
