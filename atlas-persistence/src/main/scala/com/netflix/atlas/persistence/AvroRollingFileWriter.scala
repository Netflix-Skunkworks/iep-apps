package com.netflix.atlas.persistence

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import com.netflix.atlas.core.model.Datapoint
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

/**
  * This class does the file rolling based on given conditions, and writes data in avro format. It
  * creates a ".tmp" file while writing and removes the suffix when finished and rolls over to a new
  * file.
  */
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

  override protected def writeIt(dp: Datapoint): Unit = {
    currWriter.append(toAvro(dp))
    currNumRecords += 1
  }

  override protected def shouldRollOver: Boolean = {
    currNumRecords >= maxRecords ||
    System.currentTimeMillis() - currCreatedAtMs >= maxDurationMs
  }

  override protected def finishCurrentWriter: Unit = {
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
