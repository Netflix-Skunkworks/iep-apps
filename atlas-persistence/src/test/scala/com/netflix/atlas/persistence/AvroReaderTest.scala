package com.netflix.atlas.persistence

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader

object AvroReaderTest {
  def main(args: Array[String]): Unit = {
    val dir = "./out"
    Files.walk(Paths.get(dir))
      .filter(path => Files.isRegularFile(path))
      .forEach(p => readFile(p.toFile))
  }

  private def readFile(file: File): Unit = {
    println(s"    ##### reading file: $file")
    var count = 0
    val userDatumReader = new SpecificDatumReader[AvroDatapoint](classOf[AvroDatapoint])
    val dataFileReader = new DataFileReader[AvroDatapoint](file, userDatumReader)
    while (dataFileReader.hasNext) {
      val dp = dataFileReader.next();
      //println(dp)
      count += 1
    }

    println(s"blockCount = ${dataFileReader.getBlockCount}")
    println(s"blockSize  = ${dataFileReader.getBlockSize}")
    println(s"numRecords = ${count}")

    dataFileReader.close()
    println
  }
}
