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

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.SinkShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging

// TODO handle IO failures
class RollingFileSink(
  val sinkDir: String,
  val maxRecords: Long
) extends GraphStage[SinkShape[Datapoint]]
    with StrictLogging {

  private val in = Inlet[Datapoint]("RollingFileSink.in")
  override val shape = SinkShape(in)
  require(maxRecords > 0)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new GraphStageLogic(shape) with InHandler {

      private val hourDirFormatter = DateTimeFormatter.ofPattern("'hour'-yyyyMMdd'T'HH")

      private var currentWriter: RollingFileWriter = _
      // "-1" to make sure HourlyRollOver triggered at the beginning
      private var currentHourStart: Long = -1

      override def preStart(): Unit = {
        Files.createDirectories(Paths.get(sinkDir))
        pull(in)
      }

      override def onPush(): Unit = {
        checkHourlyRollOver()
        currentWriter.write(grab(in))
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        super.completeStage()
        //TODO close all active Hourly writers
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.failStage(ex)
        //TODO close all active Hourly writers
      }

      setHandler(in, this)

      //TODO long idle will cause hourly rollover delay, and hour gap > 1, heartbeat to avoid
      private def checkHourlyRollOver() = {
        val hourStart = getHourStart(System.currentTimeMillis)
        if (hourStart > currentHourStart) {
          if (currentWriter != null) {
            currentWriter.close()
          }
          currentWriter = newHourlyFileWriter(hourStart)
          currentHourStart = hourStart
        }
      }

      private def newHourlyFileWriter(hourStart: Long): RollingFileWriter = {
        val dateTime =
          LocalDateTime.ofInstant(Instant.ofEpochMilli(hourStart), ZoneId.systemDefault())
        val hourDir = dateTime.format(hourDirFormatter)
        new RollingFileWriter(s"$sinkDir/$hourDir", maxRecords)
      }

      private def getHourStart(timestamp: Long): Long = {
        timestamp / 3600_000 * 3600_000
      }

    }
  }
}
