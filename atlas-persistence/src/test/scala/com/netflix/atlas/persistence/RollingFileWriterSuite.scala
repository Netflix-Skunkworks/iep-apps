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

class RollingFileWriterSuite extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterAll {

  private val outputDir = "./target/unitTestAvroOutput"
  private val registry = new NoopRegistry

  before {
    listFilesSorted(outputDir).foreach(_.delete()) // Clean up files if exits
    Files.createDirectories(Paths.get(outputDir))
  }

  after {
    listFilesSorted(outputDir).foreach(_.delete())
    Files.deleteIfExists(Paths.get(outputDir))
  }

  testWriterWithCodec("null")
  testWriterWithCodec("deflate")
  testWriterWithCodec("bzip2")

  // ########### Below codec's require additional library ############
  testWriterWithCodec("snappy")
  //testWriterWithCodec("xz")  // Higher ratio and slower than bzip2
  //testWriterWithCodec("zstandard")  // Seems similar to snappy

  // Write 3 datapoints, first 2 is written in file 1, rollover, and 3rd one is written in file 2
  private def testWriterWithCodec(codec: String) {
    test(s"avro writer rollover by max records - codec=$codec") {
      val rollingConf = RollingConfig(2, 12000, 12000, codec, 9, 64000)
      val hourStart = 3600000
      val hourEnd = 7200000
      val writer =
        new RollingFileWriter(s"$outputDir/codec.$codec", rollingConf, hourStart, hourEnd, registry)
      writer.initialize()
      createData(hourStart, 0, 1, 2).foreach(writer.write)
      writer.write(Datapoint(Map.empty, hourEnd, 3)) // out of range, should be ignored
      writer.close()

      // Check num of files
      val files = listFilesSorted(outputDir)
      assert(files.size == 2)

      // Check file 1 records
      val file1 = files.head
      assert(file1.getName.endsWith(".0000-0001"))
      val dpArray1 = readAvro(file1)
      assert(dpArray1.size == 2)
      assert(dpArray1(0).getValue == 0)
      assert(dpArray1(0).getTags.get("node") == "0")
      assert(dpArray1(1).getValue == 1)
      assert(dpArray1(1).getTags.get("node") == "1")

      // Check file 2 records
      val file2 = files.last
      assert(file2.getName.endsWith(".0002-0002"))
      val dpArray2 = readAvro(file2)
      assert(dpArray2.size == 1)
      assert(dpArray2(0).getValue == 2)
      assert(dpArray2(0).getTags.get("node") == "2")
    }
  }

  private def createData(startTime: Long, values: Double*): List[Datapoint] = {
    values.toList.zipWithIndex.map {
      case (v, i) =>
        val tags = Map(
          "name" -> "cpu",
          "node" -> s"$i"
        )
        Datapoint(tags, startTime + i * 1000, v, 60000)
    }
  }

  private def listFilesSorted(dir: String): List[File] = {
    val d = new File(dir)
    if (!d.exists()) {
      Nil
    } else {
      new File(dir).listFiles().filter(_.isFile).toList.sortBy(_.getName)
    }
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
