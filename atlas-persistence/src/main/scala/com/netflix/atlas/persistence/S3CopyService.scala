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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class S3CopyService @Inject()(
  val config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  private val baseDir = config.getString("atlas.persistence.local-file.base-dir")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  override def startImpl(): Unit = {
    logger.info("Starting service")
    Source
      .repeat(NotUsed)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      .flatMapMerge(Int.MaxValue, _ => Source(listAllFiles()))
      .filter(shouldCopy)
      .runForeach(copyToS3) // TODO implement a stage or sink to avoid dup copy
  }

  override def stopImpl(): Unit = {}

  private def listAllFiles(): List[File] = {
    try {
      val buf = ListBuffer.empty[File]
      Files
        .walk(Paths.get(baseDir))
        .filter(path => Files.isRegularFile(path))
        .forEach(p => buf.addOne(p.toFile.getCanonicalFile))
      buf.toList
    } catch {
      case e: Exception =>
        logger.error(s"Error listing files of $baseDir", e)
        Nil
    }
  }

  private def shouldCopy(f: File): Boolean = {
    !f.getName.endsWith(".tmp")
  }

  //TODO implement copy - print info and delete for now
  def copyToS3(f: File): Unit = {
    logger.info(s"copyToS3: ${f.getName}")
    f.delete()
  }

}
