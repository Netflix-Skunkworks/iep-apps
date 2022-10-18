/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.persistence

import com.netflix.atlas.core.model.Datapoint

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.file.CodecFactory

/**
  * Hourly writer does hourly directory rolling, and delegates actual writing to underlying
  * RollingFileWriter.
  */
class HourlyRollingWriter(
  val dataDir: String,
  val rollingConf: RollingConfig,
  val registry: Registry,
  val workerId: Int
) extends StrictLogging {

  private val msOfOneHour = 3600000

  private val baseId = registry.createId("persistence.outOfOrderEvents")
  private val lateEventsCounter = registry.counter(baseId.withTag("id", "late"))
  private val futureEventsCounter = registry.counter(baseId.withTag("id", "future"))

  private var currWriter: RollingFileWriter = _
  private var prevWriter: RollingFileWriter = _

  // Assume maxLateDuration is within 1h
  require(rollingConf.maxLateDurationMs > 0 && rollingConf.maxLateDurationMs <= msOfOneHour)

  def initialize(): Unit = {
    currWriter = createWriter(System.currentTimeMillis())
    // Create Writer for previous hour if still within limit
    if (System.currentTimeMillis() <= rollingConf.maxLateDurationMs + currWriter.startTime) {
      prevWriter = createWriter(currWriter.startTime - msOfOneHour)
    }
  }

  def close(): Unit = {
    if (currWriter != null) currWriter.close()
    if (prevWriter != null) prevWriter.close()
  }

  private def rollOverWriter(): Unit = {
    if (prevWriter != null) prevWriter.close()
    prevWriter = currWriter
    currWriter = createWriter(System.currentTimeMillis())
  }

  private def createWriter(ts: Long): RollingFileWriter = {
    val hourStart = getHourStart(ts)
    val hourEnd = hourStart + msOfOneHour
    val writer = new RollingFileWriter(
      getFilePathPrefixForHour(hourStart),
      rollingConf,
      hourStart,
      hourEnd,
      registry,
      workerId
    )
    writer.initialize()
    writer
  }

  def write(dps: List[Datapoint]): Unit = {
    dps.foreach(writeDp)
  }

  private def writeDp(dp: Datapoint): Unit = {
    val now = System.currentTimeMillis()
    checkHourRollover(now)
    checkPrevHourExpiration(now)

    if (RollingFileWriter.RolloverCheckDatapoint eq dp) {
      // check rollover for both writers
      currWriter.write(dp)
      if (prevWriter != null) prevWriter.write(dp)
    } else {
      // Range checking in order, higher possibility goes first:
      //   current hour -> previous hour -> late -> future

      if (currWriter.shouldAccept(dp)) {
        currWriter.write(dp)
      } else if (prevWriter != null && prevWriter.shouldAccept(dp)) {
        prevWriter.write(dp)
      } else if (dp.timestamp < currWriter.startTime) {
        lateEventsCounter.increment()
        logger.debug(s"found late event: $dp")
      } else {
        futureEventsCounter.increment()
        logger.debug(s"found future event: $dp")
      }
    }
  }

  private def checkHourRollover(now: Long): Unit = {
    if (now >= currWriter.endTime) {
      rollOverWriter()
    }
  }

  // Note: late arrival is only checked cross hour, not rolling time
  private def checkPrevHourExpiration(now: Long): Unit = {
    if (prevWriter != null && (now > currWriter.startTime + rollingConf.maxLateDurationMs)) {
      logger.debug(
        s"stop writer for previous hour after maxLateDuration of ${rollingConf.maxLateDurationMs} ms"
      )
      prevWriter.close()
      prevWriter = null
    }
  }

  private def getHourStart(timestamp: Long): Long = {
    timestamp / msOfOneHour * msOfOneHour
  }

  private def getFilePathPrefixForHour(hourStart: Long): String = {
    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(hourStart), ZoneOffset.UTC)
    s"$dataDir/${dateTime.format(HourlyRollingWriter.HourFormatter)}"
  }
}

object HourlyRollingWriter {

  val HourFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'00'")
  val HourStringLen: Int = 15
}

case class RollingConfig(
  maxRecords: Long,
  maxDurationMs: Long,
  maxLateDurationMs: Long,
  codec: String,
  compressionLevel: Int,
  syncInterval: Int,
  commonStrings: Map[String, Int]
) {

  // Doing config checks here to fail early for invalid values
  require(maxRecords > 0)
  require(maxDurationMs > 0)
  require(maxLateDurationMs > 0)
  CodecFactory.fromString(codec) // just for validation
  require(compressionLevel >= 1 && compressionLevel <= 9)
}
