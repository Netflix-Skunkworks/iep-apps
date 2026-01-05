/*
 * Copyright 2014-2026 Netflix, Inc.
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
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import munit.FunSuite

import scala.collection.mutable.ListBuffer

class RollingFileWriterSuite extends FunSuite {

  private val outputDir = "./target/unitTestAvroOutput"
  private val registry = new NoopRegistry
  private val commonStrings = Map("name" -> 0, "node" -> 1)

  override def beforeEach(context: BeforeEach): Unit = {
    listFilesSorted(outputDir).foreach(_.delete()) // Clean up files if exits
    Files.createDirectories(Paths.get(outputDir))
  }

  override def afterEach(context: AfterEach): Unit = {
    listFilesSorted(outputDir).foreach(_.delete())
    Files.deleteIfExists(Paths.get(outputDir))
  }

  testWriterWithCodec("null")
  testWriterWithCodec("deflate")
  testWriterWithCodec("bzip2")

  // ########### Below codec's require additional library ############
  testWriterWithCodec("snappy")
  // testWriterWithCodec("xz")  // Higher ratio and slower than bzip2
  // testWriterWithCodec("zstandard")  // Seems similar to snappy

  // Write 3 datapoints, first 2 is written in file 1, rollover, and 3rd one is written in file 2
  private def testWriterWithCodec(codec: String): Unit = {
    test(s"avro writer rollover by max records - codec=$codec") {
      val rollingConf = RollingConfig(2, 12000, 12000, codec, 9, 64000, commonStrings)
      val hourStart = 3600000
      val hourEnd = 7200000
      val writer =
        new RollingFileWriter(
          s"$outputDir/codec.$codec",
          rollingConf,
          hourStart,
          hourEnd,
          registry,
          0
        )
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
      assert(dpArray1.length == 2)
      assert(dpArray1(0).value == 0)
      assert(dpArray1(0).tags("\u0081") == "0")
      assert(dpArray1(1).value == 1)
      assert(dpArray1(1).tags("\u0081") == "1")

      // Check file 2 records
      val file2 = files.last
      assert(file2.getName.endsWith(".0002-0002"))
      val dpArray2 = readAvro(file2)
      assert(dpArray2.length == 1)
      assert(dpArray2(0).value == 2)
      assert(dpArray2(0).tags("\u0081") == "2")
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

  private def readAvro(file: File): Array[Datapoint] = {
    val genericDatumReader = new GenericDatumReader[GenericRecord](RollingFileWriter.AvroSchema)
    val dataFileReader = new DataFileReader[GenericRecord](file, genericDatumReader)
    val dpListBuf = ListBuffer.empty[GenericRecord]
    try {
      while (dataFileReader.hasNext) {
        dpListBuf.addOne(dataFileReader.next)
      }
    } finally {
      dataFileReader.close()
    }
    dpListBuf.toArray.map(record => {
      import scala.jdk.CollectionConverters.*
      Datapoint(
        record
          .get("tags")
          .asInstanceOf[java.util.Map[Utf8, Utf8]]
          .asScala
          .map { kv =>
            (kv._1.toString, kv._2.toString)
          }
          .toMap,
        record.get("timestamp").asInstanceOf[Long],
        record.get("value").asInstanceOf[Double]
      )
    })
  }

  test("ignore datapoints outside time window") {
    val rollingConf = RollingConfig(10, 12000, 12000, "null", 1, 64000, commonStrings)
    val hourStart = 1000L
    val hourEnd = 2000L
    val writer = new RollingFileWriter(
      s"$outputDir/timeWindow",
      rollingConf,
      hourStart,
      hourEnd,
      registry,
      1
    )
    writer.initialize()
    // All these are outside the window
    val dps = List(
      Datapoint(Map("name" -> "cpu", "node" -> "0"), 500, 1.0, 60000), // before window
      Datapoint(Map("name" -> "cpu", "node" -> "1"), 2000, 2.0, 60000), // at end
      Datapoint(Map("name" -> "cpu", "node" -> "2"), 3000, 3.0, 60000) // after window
    )
    dps.foreach(writer.write)
    writer.close()
    val files = listFilesSorted(outputDir)
    // No files with data should be produced
    assert(files.isEmpty)
  }

  test("delete temp file if no records written") {
    val rollingConf = RollingConfig(10, 12000, 12000, "null", 1, 64000, commonStrings)
    val hourStart = 0L
    val hourEnd = 1000L
    val writer = new RollingFileWriter(
      s"$outputDir/empty",
      rollingConf,
      hourStart,
      hourEnd,
      registry,
      2
    )
    writer.initialize()
    // Do not write any datapoints
    writer.close()
    val files = listFilesSorted(outputDir)
    // No files should remain
    assert(files.isEmpty)
  }

  test("file naming pattern includes worker id and random suffix") {
    val rollingConf = RollingConfig(10, 12000, 12000, "null", 1, 64000, commonStrings)
    val hourStart = 0L
    val hourEnd = 1000L
    val workerId = 7
    val writer = new RollingFileWriter(
      s"$outputDir/naming",
      rollingConf,
      hourStart,
      hourEnd,
      registry,
      workerId
    )
    writer.initialize()
    val dp = Datapoint(Map("name" -> "cpu", "node" -> "X"), hourStart, 1.0, 60000)
    writer.write(dp)
    writer.close()
    val files = listFilesSorted(outputDir)
    assert(files.length == 1)
    val fileName = files.head.getName
    assert(fileName.contains(s".$workerId.0000")) // workerId and seq
    assert(fileName.matches(""".*\.[A-Za-z0-9]{6}\.\d{4}-\d{4}$""")) // random string and range
  }

  test("common strings are compressed in tags") {
    val rollingConf = RollingConfig(10, 12000, 12000, "null", 1, 64000, commonStrings)
    val hourStart = 0L
    val hourEnd = 1000L
    val writer = new RollingFileWriter(
      s"$outputDir/compress",
      rollingConf,
      hourStart,
      hourEnd,
      registry,
      3
    )
    writer.initialize()
    val dp = Datapoint(Map("name" -> "cpu", "node" -> "0"), hourStart, 1.0, 60000)
    writer.write(dp)
    writer.close()
    val files = listFilesSorted(outputDir)
    assert(files.length == 1)
    val dpArray = readAvro(files.head)
    // "\u0080" = 128 (name), "\u0081" = 129 (node)
    assert(dpArray.head.tags.contains("\u0080"))
    assert(dpArray.head.tags.contains("\u0081"))
  }

  test("rollover by duration") {
    var now = 0L
    val fakeClock = () => now
    val rollingConf = RollingConfig(10, 1, 1, "null", 1, 64000, commonStrings)
    val hourStart = 0L
    val hourEnd = 10000L
    val writer = new RollingFileWriter(
      s"$outputDir/rolloverByDuration",
      rollingConf,
      hourStart,
      hourEnd,
      registry,
      4,
      clock = fakeClock // inject test clock
    )
    writer.initialize()
    val dp1 = Datapoint(Map("name" -> "cpu", "node" -> "0"), hourStart, 1.0, 60000)
    writer.write(dp1)
    now += 2 // simulate time passage
    val dp2 = Datapoint(Map("name" -> "cpu", "node" -> "1"), hourStart + 1000, 2.0, 60000)
    writer.write(dp2)
    writer.close()
    val files = listFilesSorted(outputDir)
    assert(files.length == 2)
  }
}
