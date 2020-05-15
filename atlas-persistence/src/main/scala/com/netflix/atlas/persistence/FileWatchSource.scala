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
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchService

import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.OutHandler
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

// Source that watches a directory and emits all exiting and new files
// TODO scan periodically(timer) to process long idle .tmp files
class FileWatchSource(directory: String) extends GraphStage[SourceShape[File]] with StrictLogging {
  val out: Outlet[File] = Outlet("FileWatchSource.out")
  override val shape: SourceShape[File] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      private var watchService: WatchService = _
      private val queue = mutable.Queue[File]()

      setHandler(out, this)

      override def preStart(): Unit = {
        logger.debug("starting WatchService")
        watchService = FileSystems.getDefault.newWatchService
        Paths.get(directory).register(watchService, StandardWatchEventKinds.ENTRY_CREATE)
        checkExistingFiles
      }

      override def postStop(): Unit = {
        logger.debug("closing WatchService")
        if (watchService != null) watchService.close()
      }

      override def onPull(): Unit = {
        while (queue.isEmpty) {
          checkNewFiles
        }
        push(out, queue.dequeue())
      }

      private def checkExistingFiles(): Unit = {
        try {
          new File(directory)
            .listFiles()
            .filter(_.isFile)
            .toList
            .foreach(f => {
              logger.debug(s"Adding existing file: $f")
              queue.enqueue(f)
            })
        } catch {
          case e: Exception => logger.error("error adding existing files", e)
        }
      }

      private def checkNewFiles(): Unit = {
        val watchKey = watchService.take() // Blocking call
        if (watchKey.isValid) {
          import scala.jdk.CollectionConverters._
          watchKey
            .pollEvents()
            .asScala
            .foreach(
              event =>
                event match {
                  case pathEvent: WatchEvent[Path] => {
                    val path = pathEvent.context()
                    logger.debug(s"File Watch found new file: $path")
                    queue.enqueue(new File(s"$directory/$path"))
                  }
                  case we: WatchEvent[_] => {
                    logger.warn(s"Found unknown event $we")
                  }
                }
            )
          watchKey.reset()
        }
      }
    }
}
