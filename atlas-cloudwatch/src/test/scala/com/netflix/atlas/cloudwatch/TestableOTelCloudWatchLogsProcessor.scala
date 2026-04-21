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

import com.typesafe.config.ConfigFactory

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/**
 * Testable version of OTelCloudWatchLogsProcessor that:
 *  - uses a stub OTEL sink to capture sent logs.
 */
class TestableOTelCloudWatchLogsProcessor(
  val sinkStub: StubOtelLogSink = new StubOtelLogSink
) extends OTelCloudWatchLogsProcessor(
      ConfigFactory.load().getConfig("atlas.cloudwatch.logs"),
      sinkStub
    ) {

  // (group, pattern, sample)
  private val newPatternsQueue =
    new ConcurrentLinkedQueue[(String, String, String)]()

  /** Expose captured patterns for assertions. */
  def newPatterns: List[(String, String, String)] =
    newPatternsQueue.asScala.toList

  /** Expose captured OTEL logs for assertions. */
  def sentLogs: List[OtelLog] =
    sinkStub.logs
}

class StubOtelLogSink extends OtelLogSink {

  private val queue = new ConcurrentLinkedQueue[OtelLog]()

  override def send(log: OtelLog): Unit = {
    queue.add(log)
  }

  def logs: List[OtelLog] =
    queue.asScala.toList
}
