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

import org.apache.pekko.Done
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.Balance
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.GraphDSL
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Merge
import org.apache.pekko.stream.scaladsl.RestartFlow
import org.apache.pekko.stream.scaladsl.Sink
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.pekko.StreamOps.SourceQueue
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json3.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.nio.file.attribute.FileTime
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Using

class LocalFilePersistService(
  val config: Config,
  val registry: Registry,
  // S3CopyService is actually NOT used by this service, it is here just to guarantee that the
  // shutdown callback (stopImpl) of this service is invoked before S3CopyService's
  val s3CopyService: S3CopyService,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  private val queueSize = config.getInt("atlas.persistence.queue-size")

  // Set worker size based on number of cores
  private val writeWorkerSize = getWorkerSize
  logger.info(s"writeWorkerSize=$writeWorkerSize")
  // To expose in admin portal for convenience
  System.setProperty("persistence.writeWorkerSize", writeWorkerSize.toString)

  private val fileConfig = config.getConfig("atlas.persistence.local-file")
  private val dataDir = fileConfig.getString("data-dir")

  private val commonStrings: Map[String, Int] = {
    val is = getClass.getClassLoader.getResourceAsStream("common-strings.json")
    if (is == null) Map.empty
    else Using.resource(is)(is => Json.decode[Map[String, Int]](is))
  }

  private val rollingConf = RollingConfig(
    fileConfig.getLong("max-records"),
    fileConfig.getDuration("max-duration").toMillis,
    fileConfig.getDuration("max-late-duration").toMillis,
    fileConfig.getString("avro-codec"),
    fileConfig.getInt("avro-deflate-compressionLevel"),
    fileConfig.getInt("avro-syncInterval"),
    commonStrings
  )

  private var queue: SourceQueue[List[Datapoint]] = _
  private var flowComplete: Future[Done] = _

  override def startImpl(): Unit = {
    logger.info("Starting service")

    // Make sure we have permission to write to the configured directory or propagate
    // the IOException
    Files.createDirectories(Paths.get(dataDir))
    val touchFile = Paths.get(dataDir, "permissionTest")
    if (!Files.exists(touchFile)) {
      Files.createFile(touchFile)
    }
    Files.setLastModifiedTime(
      touchFile,
      FileTime.from(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    )
    Files.delete(touchFile)

    val (q, f) = StreamOps
      .blockingQueue[List[Datapoint]](registry, "LocalFilePersistService", queueSize)
      .via(balancer(getRollingFileFlow, writeWorkerSize))
      .toMat(Sink.ignore)(Keep.both)
      .run()
    queue = q
    flowComplete = f
  }

  override def isHealthy: Boolean = {
    super.isHealthy && queue != null && queue.isOpen
  }

  // Set worker size to half of core size is optimal based on benchmark on M5 instances
  private def getWorkerSize: Int = {
    val cores = Runtime.getRuntime.availableProcessors()
    Math.max(1, cores / 2)
  }

  private def getRollingFileFlow(workerId: Int): Flow[List[Datapoint], NotUsed, NotUsed] = {
    import scala.concurrent.duration.*
    RestartFlow.withBackoff(
      RestartSettings(
        minBackoff = 1.second,
        maxBackoff = 3.seconds,
        randomFactor = 0
      )
    ) { () =>
      Flow.fromGraph(
        new RollingFileFlow(dataDir, rollingConf, registry, workerId)
      )
    }
  }

  def balancer[In, Out](
    workerFlowFactory: Int => Flow[In, Out, NotUsed],
    workerCount: Int
  ): Flow[In, Out, NotUsed] = {
    if (workerCount == 1) {
      // Don't add overhead of balancer and async boundary for single worker
      workerFlowFactory(0)
    } else {
      import org.apache.pekko.stream.scaladsl.GraphDSL.Implicits.*
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        val balancer = b.add(Balance[In](workerCount))
        val merge = b.add(Merge[Out](workerCount))
        for (i <- 0 until workerCount) {
          balancer ~> workerFlowFactory(i).async ~> merge
        }
        FlowShape(balancer.in, merge.out)
      })
    }
  }

  // This service should stop the Pekko flow when application is shutdown gracefully, and let
  // S3CopyService do the cleanup. It should trigger:
  //   1. stop taking more data points (monitor droppedQueueClosed)
  //   2. close current file writer so that last file is ready to copy to s3
  override def stopImpl(): Unit = {
    logger.info("Stopping service")
    queue.complete()
    Await.result(flowComplete, Duration.Inf)
    logger.info("Stopped service")
  }

  def persist(dp: List[Datapoint]): Unit = {
    queue.offer(dp)
  }
}
