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
import java.util.concurrent.ConcurrentHashMap

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.concurrent.duration._

class S3CopyService @Inject()(
  val config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  private val dataDir = config.getString("atlas.persistence.local-file.data-dir")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  private var killSwitch: KillSwitch = _
  private val s3Config = config.getConfig("atlas.persistence.s3")
  private val bucket = s3Config.getString("bucket")
  private val region = s3Config.getString("region")
  private val prefix = s3Config.getString("prefix")
  private val cleanupTimeoutMs = s3Config.getDuration("cleanup-timeout").toMillis
  private val maxInactiveMs = s3Config.getDuration("max-inactive-duration").toMillis

  private val maxFileDurationMs =
    config.getDuration("atlas.persistence.local-file.max-duration").toMillis

  require(
    maxInactiveMs > maxFileDurationMs,
    "`max-inactive-duration` MUST be longer than `max-duration`, otherwise file may be renamed before normal write competes"
  )

  // Tracking files which is being processed to avoid duplication
  private val activeFiles = new ConcurrentHashMap[String, KillSwitch]

  // TODO handle .tmp
  override def startImpl(): Unit = {
    logger.info("Starting service")
    killSwitch = Source
      .tick(1.second, 5.seconds, NotUsed)
      .viaMat(KillSwitches.single)(Keep.right)
      .flatMapMerge(Int.MaxValue, _ => Source(FileUtil.listFiles(new File(dataDir))))
      .filter(shouldProcess)
      // S3CopySink handles flow restart for each file so no need to use RestartSink here
      .toMat(new S3CopySink(bucket, region, prefix, activeFiles, registry, system))(Keep.left)
      .run()
  }

  def isInactive(f: File): Boolean = {
    try {
      System.currentTimeMillis > f.lastModified() + maxInactiveMs
    } catch {
      case e: Exception =>
        logger.error(s"Error get lastModified for file $f", e)
        false
    }
  }

  def shouldProcess(f: File): Boolean = {
    if (activeFiles.containsKey(f.getName)) {
      logger.debug(s"Should NOT process $f: being processed already")
      false
    } else if (FileUtil.isTmpFile(f)) {
      if (isInactive(f)) {
        logger.warn(s"Should process: .tmp file but inactive")
        true
      } else {
        logger.debug(s"Should NOT process $f: .tmp file")
        false
      }
    } else {
      logger.debug(s"Should NOT process $f: regular file")
      true
    }
  }

  override def stopImpl(): Unit = {
    waitForCleanup()
    logger.info("Stopping service")
    if (killSwitch != null) killSwitch.shutdown()
  }

  private def waitForCleanup(): Unit = {
    logger.info("Waiting for cleanup")
    val start = System.currentTimeMillis
    while (!listFiles.isEmpty) {
      if (System.currentTimeMillis() > start + cleanupTimeoutMs) {
        logger.error("Cleanup timeout")
        return
      }
      Thread.sleep(1000)
    }
    logger.info("Cleanup done")
  }

  private def listFiles: List[File] = {
    try {
      new File(dataDir)
        .listFiles()
        .filter(_.isFile)
        .toList
    } catch {
      case e: Exception => {
        logger.error(s"Error listing files in $dataDir", e)
        throw e
      }
    }
  }
}
