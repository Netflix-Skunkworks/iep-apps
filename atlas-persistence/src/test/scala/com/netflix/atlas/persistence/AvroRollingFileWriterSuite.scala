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

import com.netflix.atlas.core.model.Datapoint
import com.netflix.spectator.api.NoopRegistry
import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class AvroRollingFileWriterSuite extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterAll {

  private val outputDir = "./target/unitTestAvroOutput"
  private val registry = new NoopRegistry

  before {
    Files.createDirectories(Paths.get(outputDir))
  }

  after {
    listFilesSorted(outputDir).foreach(_.delete())
    Files.deleteIfExists(Paths.get(outputDir))
  }

  // Write 3 datapoints, first 2 is written in file 1, rollover, and 3rd one is written in file 2
  test("avro writer rollover by max records") {
    val writer = new AvroRollingFileWriter(s"$outputDir/prefix", 2, 60000, registry)
    writer.initialize()
    createData(0, 1, 2).foreach(writer.write)
    writer.close()

    // Check num of files
    val files = listFilesSorted(outputDir)
    assert(files.size == 2)

    // Check file 1 records
    val file1 = files.head
    val dpArray1 = readAvro(file1)
    assert(dpArray1.size == 2)
    assert(dpArray1(0).getValue == 0)
    assert(dpArray1(0).getTags.get("node") == "0")
    assert(dpArray1(1).getValue == 1)
    assert(dpArray1(1).getTags.get("node") == "1")

    // Check file 2 records
    val file2 = files.last
    val dpArray2 = readAvro(file2)
    assert(dpArray2.size == 1)
    assert(dpArray2(0).getValue == 2)
    assert(dpArray2(0).getTags.get("node") == "2")
  }

  private def createData(values: Double*): List[Datapoint] = {
    values.toList.zipWithIndex.map {
      case (v, i) =>
        val tags = Map(
          "name" -> "cpu",
          "node" -> s"$i"
        )
        Datapoint(tags, 12345, v, 60000)
    }
  }

  private def listFilesSorted(dir: String): List[File] = {
    new File(dir).listFiles().filter(_.isFile).toList.sortBy(_.getName)
  }

  private def readAvro(file: File): Array[AvroDatapoint] = {
    val userDatumReader = new SpecificDatumReader[AvroDatapoint](classOf[AvroDatapoint])
    val dataFileReader = new DataFileReader[AvroDatapoint](file, userDatumReader)
    val dpListBuf = ListBuffer.empty[AvroDatapoint]
    try {
      while (dataFileReader.hasNext) {
        dpListBuf.addOne(dataFileReader.next)
      }
    } finally {
      dataFileReader.close()
    }
    dpListBuf.toArray
  }
}
