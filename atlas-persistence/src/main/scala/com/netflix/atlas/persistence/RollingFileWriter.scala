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
import java.nio.file.Paths

import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

import scala.util.Random

trait RollingFileWriter extends StrictLogging {

  def initialize(): Unit = newWriter

  def write(dp: Datapoint): Unit = {
    if (shouldRollOver) {
      rollOver
      newWriter
    }
    writeImpl(dp)
  }

  // Assuming rollOver closes current file
  def close(): Unit = rollOver

  protected[this] def newWriter(): Unit
  protected[this] def writeImpl(dp: Datapoint): Unit
  protected[this] def shouldRollOver: Boolean
  protected[this] def rollOver(): Unit
}

//TODO handl IO failures
class AvroRollingFileWriter(
  val filePathPrefix: String,
  val maxRecords: Long,
  val maxDurationMs: Long
) extends RollingFileWriter {

  // These "curr*" fields track status of the current file writer
  private var currFile: String = _
  private var currWriter: DataFileWriter[AvroDatapoint] = _
  private var currCreatedAtMs: Long = 0
  private var currNumRecords: Long = 0

  private var nextFileSeqId: Long = 0

  private val tmpFileSuffix = ".tmp"

  override protected def newWriter(): Unit = {
    val newFile = getNextTmpFilePath
    logger.info(s"New avro file: $newFile")
    val dataFileWriter = new DataFileWriter[AvroDatapoint](
      new SpecificDatumWriter[AvroDatapoint](classOf[AvroDatapoint])
    )
    // Possible to use API that takes OutputStream to track file size if needed
    dataFileWriter.create(AvroDatapoint.getClassSchema, new File(newFile))

    // Update tracking fields
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
    currNumRecords >= maxRecords || System.currentTimeMillis() - currCreatedAtMs >= maxDurationMs
  }

  override protected def rollOver: Unit = {
    currWriter.close()

    if (currNumRecords == 0) {
      // Simply delete the file if no records written
      logger.debug(s"deleting file with 0 record: ${currFile}")
      FileUtil.delete(Paths.get(currFile), logger)
    } else {
      // Rename file, removing tmp file suffix
      logger.debug(s"rolling over file $currFile")
      FileUtil.move(
        Paths.get(currFile),
        Paths.get(currFile.substring(0, currFile.length - tmpFileSuffix.length)),
        logger
      )
    }
  }

  // Example file name: 2020051003.i-localhost.1.XkvU3A.tmp
  // The random string suffix is to avoid file name conflict when server restarts
  private def getNextTmpFilePath: String = {
    s"$filePathPrefix.$getInstanceId.$nextFileSeqId.$getRandomStr$tmpFileSuffix"
  }

  private def getInstanceId: String = {
    sys.env.getOrElse("NETFLIX_INSTANCE_ID", "i-localhost")
  }

  private def getRandomStr: String = {
    Random.alphanumeric.take(6).mkString
  }

  private def toAvro(dp: Datapoint): AvroDatapoint = {
    import scala.jdk.CollectionConverters._
    AvroDatapoint.newBuilder
      .setTags(dp.tags.asJava)
      .setTimestamp(dp.timestamp)
      .setValue(dp.value)
      .build
  }
}
