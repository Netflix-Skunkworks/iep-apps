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