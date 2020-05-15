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

import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.SinkShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import com.netflix.atlas.core.model.Datapoint
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

class RollingFileSink(
  val dataDir: String,
  val maxRecords: Long,
  val maxDurationMs: Long,
  val maxLateDuration: Long,
  val registry: Registry
) extends GraphStage[SinkShape[Datapoint]]
    with StrictLogging {

  private val in = Inlet[Datapoint]("RollingFileSink.in")
  override val shape = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new GraphStageLogic(shape) with InHandler {

      private var hourlyWriter: HourlyRollingWriter = _
      private val writerFactory: String => RollingFileWriter =
        filePathPrefix => new AvroRollingFileWriter(filePathPrefix, maxRecords, maxDurationMs)

      override def preStart(): Unit = {
        logger.info(s"creating sink directory: $dataDir")
        Files.createDirectories(Paths.get(dataDir))

        hourlyWriter = new HourlyRollingWriter(dataDir, maxLateDuration, writerFactory, registry)
        hourlyWriter.initialize
        pull(in)
      }

      // TODO timer to trigger rollover in case of long idle
      override def onPush(): Unit = {
        hourlyWriter.write(grab(in))
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        super.completeStage()
        hourlyWriter.close()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.failStage(ex)
        hourlyWriter.close()
      }

      setHandler(in, this)
    }
  }
}
