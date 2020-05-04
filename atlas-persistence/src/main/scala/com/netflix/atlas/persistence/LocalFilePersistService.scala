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

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.StreamOps.SourceQueue
import com.netflix.atlas.core.model.Datapoint
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

class LocalFilePersistService @Inject()(
  val config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {
  implicit val ec = scala.concurrent.ExecutionContext.global
  implicit val mat = ActorMaterializer()

  private val queueSize = config.getInt("atlas.persistence.queue-size")
  private val baseDir = config.getString("atlas.persistence.local-file.base-dir")
  private val maxRecords = config.getLong("atlas.persistence.local-file.max-records")
  private val maxDurationMs =
    config.getDuration("atlas.persistence.local-file.max-duration").toMillis

  require(queueSize > 0)
  require(maxRecords > 0)
  require(maxDurationMs > 0)

  private val sinkDirFormatter = DateTimeFormatter.ofPattern("'sink'-yyyyMMdd'T'HHmmss")
  // Sink dir should be unique to avoid conflict when server restarts
  private val sinkDir = s"$baseDir/${LocalDateTime.now.format(sinkDirFormatter)}"

  private var killSwitch: KillSwitch = _
  private var queue: SourceQueue[Datapoint] = _

  override def startImpl(): Unit = {
    logger.info("Starting service")
    val (q, k) = StreamOps
      .blockingQueue[Datapoint](registry, "LocalFilePersistService", queueSize)
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(new RollingFileSink(sinkDir, maxRecords, maxDurationMs))(Keep.left)
      .run
    killSwitch = k
    queue = q
  }

  override def stopImpl(): Unit = {
    logger.info("Stopping service")
    if (killSwitch != null) killSwitch.shutdown()
    Thread.sleep(1000)
  }

  def save(dp: Datapoint): Unit = {
    queue.offer(dp)
  }
}
