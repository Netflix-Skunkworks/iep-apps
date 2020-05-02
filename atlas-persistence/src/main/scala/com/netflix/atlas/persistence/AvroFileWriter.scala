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

import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

class AvroFileWriter(val file: String) extends StrictLogging {

  private val datumWriter = new SpecificDatumWriter[AvroDatapoint](classOf[AvroDatapoint])
  private val dataFileWriter = new DataFileWriter[AvroDatapoint](datumWriter)
  private var numRecords: Long = 0

  def init(): Unit = {
    logger.info(s"New output file: $file")
    dataFileWriter.create(AvroDatapoint.getClassSchema, new File(file))
  }

  // TODO Suppress error with metrics
  def write(dp: Datapoint): Unit = {
    dataFileWriter.append(toAvro(dp))
    numRecords += 1
  }

  def numRecordsWritten: Long = numRecords

  def close(): Unit = {
    try {
      dataFileWriter.close()
    } catch {
      case e: Exception => logger.error(s"Error closing avro file $file", e)
    }
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
