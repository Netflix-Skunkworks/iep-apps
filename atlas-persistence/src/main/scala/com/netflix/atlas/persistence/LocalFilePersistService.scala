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

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RestartFlow
import akka.stream.scaladsl.Sink
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.StreamOps.SourceQueue
import com.netflix.atlas.core.model.Datapoint
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.inject.Singleton

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

@Singleton
class LocalFilePersistService @Inject()(
  val config: Config,
  val registry: Registry,
  // S3CopyService is actually NOT used by this service, it is here just to guarantee that the
  // shutdown callback (stopImpl) of this service is invoked before S3CopyService's
  val s3CopyService: S3CopyService,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {
  implicit val ec = scala.concurrent.ExecutionContext.global
  implicit val mat = ActorMaterializer()

  private val queueSize = config.getInt("atlas.persistence.queue-size")

  private val fileConfig = config.getConfig("atlas.persistence.local-file")
  private val dataDir = fileConfig.getString("data-dir")
  private val rollingConf = RollingConfig(
    fileConfig.getLong("max-records"),
    fileConfig.getDuration("max-duration").toMillis,
    fileConfig.getDuration("max-late-duration").toMillis,
    fileConfig.getString("avro-codec"),
    fileConfig.getInt("avro-syncInterval")
  )

  private var queue: SourceQueue[Datapoint] = _
  private var flowComplete: Future[Done] = _

  override def startImpl(): Unit = {
    logger.info("Starting service")
    val (q, f) = StreamOps
      .blockingQueue[Datapoint](registry, "LocalFilePersistService", queueSize)
      .via(getRollingFileFlow)
      .toMat(Sink.ignore)(Keep.both)
      .run
    queue = q
    flowComplete = f
  }

  private def getRollingFileFlow(): Flow[Datapoint, NotUsed, NotUsed] = {
    import scala.concurrent.duration._
    RestartFlow.withBackoff(
      minBackoff = 1.second,
      maxBackoff = 3.seconds,
      randomFactor = 0,
      maxRestarts = -1
    ) { () =>
      Flow.fromGraph(
        new RollingFileFlow(dataDir, rollingConf, registry)
      )
    }
  }

  // This service should stop the Akka flow when application is shutdown gracefully, and let
  // S3CopyService do the cleanup. It should trigger:
  //   1. stop taking more data points (monitor droppedQueueClosed)
  //   2. close current file writer so that last file is ready to copy to s3
  override def stopImpl(): Unit = {
    logger.info("Stopping service")
    queue.complete()
    Await.result(flowComplete, Duration.Inf)
    logger.info("Stopped service")
  }

  def persist(dp: Datapoint): Unit = {
    queue.offer(dp)
  }
}
