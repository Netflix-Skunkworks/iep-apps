package com.netflix.atlas.persistence

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging

/**
  * Hourly writer does hourly directory rolling, and delegates actual writing to underlying
  * RollingFileWriter.
  */
class HourlyRollingWriter(
  dataDir: String,
  maxLateDuration: Long,
  writerFactory: String => RollingFileWriter
) extends StrictLogging {

  private val hourlyDirFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HH")
  private val msOfOneHour = 3600000

  private var currInfo: RangeWriter = _
  private var prevInfo: RangeWriter = _

  // Assume maxLateDuration is within 1h
  require(maxLateDuration > 0 && maxLateDuration <= msOfOneHour)

  def initialize(): Unit = {
    Files.createDirectories(Paths.get(dataDir))
    newWriter
  }

  def close(): Unit = {
    if (currInfo != null) currInfo.writer.close()
    if (prevInfo != null) prevInfo.writer.close()
  }

  private def newWriter(): Unit = {
    if (prevInfo != null) prevInfo.writer.close
    prevInfo = currInfo
    currInfo = createWriterInfo(System.currentTimeMillis())
  }

  private def createWriterInfo(ts: Long): RangeWriter = {
    val hourStart = getHourStart(ts)
    val hourEnd = hourStart + msOfOneHour
    val writer = writerFactory(getHourDir(hourStart))
    writer.initialize
    RangeWriter(writer, hourStart, hourEnd)
  }

  //Check time range and route data to the right writer
  def write(dp: Datapoint): Unit = {
    val t = dp.timestamp

    // Rollover hour
    if (t >= currInfo.endTime) {
      newWriter
    }

    // Stop writing for previous hour if time has passed hour start for more than maxLateDuration
    if (prevInfo != null && (t - currInfo.startTime > maxLateDuration)) {
      logger.info("late rollover")
      prevInfo.writer.close
      prevInfo = null
    }

    // Range checking order: higher possibility first
    if (currInfo.inRange(t)) {
      currInfo.write(dp)
      return
    }

    if (prevInfo != null && prevInfo.inRange(t)) {
      prevInfo.write(dp)
      return
    }

    if (t < currInfo.startTime) {
      //TODO record event too late
      logger.info("late event!")
    } else {
      //TODO record event in future
      logger.info("future event")
    }

  }

  private def getHourStart(timestamp: Long): Long = {
    timestamp / msOfOneHour * msOfOneHour
  }

  private def getHourDir(hourStart: Long): String = {
    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(hourStart), ZoneId.systemDefault())
    s"$dataDir/${dateTime.format(hourlyDirFormatter)}"
  }

  case class RangeWriter(
    writer: RollingFileWriter,
    startTime: Long,
    endTime: Long
  ) {

    def write(dp: Datapoint): Unit = {
      writer.write(dp)
    }

    def inRange(ts: Long): Boolean = {
      ts >= startTime && ts < endTime
    }
  }
}
