package com.netflix.atlas.persistence

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging

// Not thread safe, designed to be used in a single-threaded context.
class RollingFileWriter(val dataDir: String, val maxRecords: Long) extends StrictLogging {
  private var initialized = false
  private var nextFileSeqId: Long = 0
  private var fileWriter: AvroFileWriter = _

  def write(dp: Datapoint): Unit = {
    checkInitialize // lazy init till there's actually something to write
    checkRollOver
    fileWriter.write(dp)
  }

  private def checkInitialize = {
    if (!initialized) {
      Files.createDirectories(Paths.get(dataDir))
      newFileWriter
      initialized = true
    }
  }

  def close(): Unit = {
    if (fileWriter != null) {
      rollOver()
    }
  }

  private def checkRollOver: Unit = {
    if (shouldRollOver) {
      rollOver
      newFileWriter
    }
  }

  private def shouldRollOver = {
    fileWriter.numRecordsWritten >= maxRecords
  }

  private def rollOver(): Unit = {
    fileWriter.close()

    //Just delete the file if 0 records written
    if (fileWriter.numRecordsWritten == 0) {
      Files.delete(Paths.get(fileWriter.file))
      return
    }

    val src = Paths.get(fileWriter.file)
    val dest = Paths.get(fileWriter.file.substring(0, fileWriter.file.length - ".tmp".length))
    logger.info(s"Rolling over file from $src to $dest")
    Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE)
  }

  private def newFileWriter: Unit = {
    fileWriter = new AvroFileWriter(getNextTmpFilePath)
    fileWriter.init()
    nextFileSeqId += 1
  }

  private def getNextTmpFilePath: String = {
    s"$dataDir/data.avro.$nextFileSeqId.tmp"
  }
}
