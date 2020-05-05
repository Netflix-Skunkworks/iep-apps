/*
 * Copyright 2014-2020 Netflix, Inc.
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

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

//TODO handl IO failures
trait RollingFileWriter extends StrictLogging {
  def initialize: Unit

  def write(dp: Datapoint): Unit = {
    if (shouldRollOver) {
      rollOver
      newWriter
    }
    writeImpl(dp)
  }

  def close(): Unit = rollOver // Assuming rollOver closes current file

  protected[this] def writeImpl(dp: Datapoint): Unit
  protected[this] def rollOver: Unit
  protected[this] def newWriter: Unit
  protected[this] def shouldRollOver: Boolean
}

class AvroRollingFileWriter(val dataDir: String, val maxRecords: Long, val maxDurationMs: Long)
    extends RollingFileWriter {

  // These "curr*" fields track status of the current file writer
  private var currFile: String = _
  private var currWriter: DataFileWriter[AvroDatapoint] = _
  private var currCreatedAtMs: Long = 0
  private var currNumRecords: Long = 0

  private var nextFileSeqId: Long = 0

  override def initialize(): Unit = {
    Files.createDirectories(Paths.get(dataDir))
    newWriter()
  }

  override protected def newWriter() = {
    val newFile = getNextTmpFilePath
    logger.info(s"New avro file: $newFile")
    val dataFileWriter = new DataFileWriter[AvroDatapoint](
      new SpecificDatumWriter[AvroDatapoint](classOf[AvroDatapoint])
    )
    // Possible to use API that takes OutputStream to track file size if needed
    dataFileWriter.create(AvroDatapoint.getClassSchema, new File(newFile))

    // Reset tracking fields
    currFile = newFile
    currWriter = dataFileWriter
    currCreatedAtMs = System.currentTimeMillis()
    currNumRecords = 0

    nextFileSeqId += 1
  }

  override protected def writeImpl(dp: Datapoint): Unit = {
    currWriter.append(toAvro(dp))
    currNumRecords += 1
  }

  override protected def shouldRollOver: Boolean = {
    currNumRecords >= maxRecords ||
    System.currentTimeMillis() - currCreatedAtMs >= maxDurationMs
  }

  override protected def rollOver: Unit = {
    currWriter.close()

    //Just delete the file if no records written
    if (currNumRecords == 0) {
      logger.info(s"deleting file with 0 record: ${currFile}")
      Files.delete(Paths.get(currFile))
      return
    }
    // Rename file, removing .tmp
    val src = Paths.get(currFile)
    val dest = Paths.get(currFile.substring(0, currFile.length - ".tmp".length))
    logger.info(s"Rolling over file from $src to $dest")
    Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE)
  }

  private def getNextTmpFilePath: String = {
    s"$dataDir/avro.$nextFileSeqId.tmp"
  }

  private def toAvro(dp: Datapoint): AvroDatapoint = {
    import scala.jdk.CollectionConverters._
    AvroDatapoint.newBuilder
      .setTags(dp.tags.asJava.asInstanceOf[java.util.Map[CharSequence, CharSequence]])
      .setTimestamp(dp.timestamp)
      .setValue(dp.value)
      .build
  }
}

/**
  * Hourly writer does hourly directory rolling, and delicate actual writing to underlying
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
  override protected def writeImpl(dp: Datapoint): Unit = {
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

  override protected def rollOver: Unit = {
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
