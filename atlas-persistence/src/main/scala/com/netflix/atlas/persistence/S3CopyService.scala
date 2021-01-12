/*
 * Copyright 2014-2021 Netflix, Inc.
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
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.inject.Singleton

import scala.concurrent.duration._
import scala.util.Using

@Singleton
class S3CopyService @Inject()(
  val config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  private val dataDir = config.getString("atlas.persistence.local-file.data-dir")

  private var killSwitch: KillSwitch = _
  private val s3Config = config.getConfig("atlas.persistence.s3")

  private val cleanupTimeoutMs = s3Config.getDuration("cleanup-timeout").toMillis
  private val maxInactiveMs = s3Config.getDuration("max-inactive-duration").toMillis
  private val maxFileDurationMs =
    config.getDuration("atlas.persistence.local-file.max-duration").toMillis

  require(
    maxInactiveMs > maxFileDurationMs,
    "`max-inactive-duration` MUST be longer than `max-duration`, otherwise file may be renamed before normal write competes"
  )

  override def startImpl(): Unit = {
    logger.info("Starting service")
    killSwitch = Source
      .tick(1.second, 5.seconds, NotUsed)
      .viaMat(KillSwitches.single)(Keep.right)
      .flatMapMerge(Int.MaxValue, _ => Source(FileUtil.listFiles(new File(dataDir))))
      .toMat(new S3CopySink(s3Config, registry, system))(Keep.left)
      .run()
  }

  override def stopImpl(): Unit = {
    logger.info("Stopping service")
    waitForCleanup()
    if (killSwitch != null) killSwitch.shutdown()
  }

  private def waitForCleanup(): Unit = {
    logger.info("Waiting for cleanup")
    val start = System.currentTimeMillis
    while (hasMoreFiles) {
      if (System.currentTimeMillis() > start + cleanupTimeoutMs) {
        logger.error("Cleanup timeout")
        return
      }
      Thread.sleep(1000)
    }
    logger.info("Cleanup done")
  }

  private def hasMoreFiles: Boolean = {
    try {
      Using.resource(Files.list(Paths.get(dataDir))) { dir =>
        dir.anyMatch(f => Files.isRegularFile(f))
      }
    } catch {
      case e: Exception => {
        logger.error(s"Error checking hasMoreFiles in $dataDir", e)
        true // Assuming there's more files on error to retry
      }
    }
  }
}
