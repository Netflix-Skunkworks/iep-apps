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

import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader

// Read metadata for all avro files in given directory
object AvroTest {

  def main(args: Array[String]): Unit = {
    val dir = args(0)
    Files
      .walk(Paths.get(dir))
      .filter(path => Files.isRegularFile(path))
      .forEach(p => readFile(p.toFile))
  }

  private def readFile(file: File): Unit = {
    println(s"##### Reading file: $file")
    var count = 0
    val userDatumReader = new SpecificDatumReader[AvroDatapoint](classOf[AvroDatapoint])
    val dataFileReader = new DataFileReader[AvroDatapoint](file, userDatumReader)
    while (dataFileReader.hasNext) {
      dataFileReader.next()
      count += 1
      if (count < 4) {
        println(s"    blockSize  = ${dataFileReader.getBlockSize}")
      }
    }

    println(s"    numRecords = $count")

    dataFileReader.close()
    println
  }
}
