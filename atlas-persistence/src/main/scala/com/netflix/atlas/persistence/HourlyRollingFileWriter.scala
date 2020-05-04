package com.netflix.atlas.persistence

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.netflix.atlas.core.model.Datapoint

/**
  * This class does hourly directory rolling, and delicate actual writing to underlying
  * RollingFileWriter.
  *
  * Note: In order to avoid time gap, the exact same timestamp has to be used for updateHourStartEnd
  * and newWriter, especially during initialization, so here class fields(currHourStart) is used to
  * pass around timestamp rather than getting system time at different places.
  *
  */
// TODO late events with hour bucketing
class HourlyRollingFileWriter(
  dataDir: String,
  writerFactory: String => RollingFileWriter
) extends RollingFileWriter {

  private val hourlyDirFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HH")
  private val msOfOneHour = 3600000

  private var currWriter: RollingFileWriter = _
  private var currHourStart: Long = _
  private var currHourEnd: Long = _

  override def initialize: Unit = {
    Files.createDirectories(Paths.get(dataDir))
    updateHourStartEnd(System.currentTimeMillis())
    newWriter
  }

  override protected def newWriter: Unit = {
    val writer = writerFactory(getHourDir(currHourStart))
    writer.initialize
    currWriter = writer
  }

  //Range check should be done outside of this class
  override protected def writeIt(dp: Datapoint): Unit = {
    currWriter.write(dp)
  }

  override protected def shouldRollOver: Boolean = {
    // Pull clock only once in this method
    val ts = System.currentTimeMillis
    val ready = ts >= currHourEnd
    if (ready) {
      updateHourStartEnd(ts)
    }
    ready
  }

  private def updateHourStartEnd(ts: Long) = {
    currHourStart = getHourStart(ts)
    currHourEnd = currHourStart + msOfOneHour
  }

  override protected def finishCurrentWriter: Unit = {
    currWriter.close()
    // No need to rename hour dir
  }

  private def getHourStart(timestamp: Long): Long = {
    timestamp / msOfOneHour * msOfOneHour
  }

  private def getHourDir(hourStart: Long): String = {
    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(hourStart), ZoneId.systemDefault())
    s"$dataDir/${dateTime.format(hourlyDirFormatter)}"
  }
}
