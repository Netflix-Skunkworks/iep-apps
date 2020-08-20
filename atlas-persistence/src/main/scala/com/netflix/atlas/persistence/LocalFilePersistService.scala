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
import akka.stream.FlowShape
import akka.stream.scaladsl.Balance
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Merge
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
import scala.concurrent.ExecutionContextExecutor
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
  implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

  private val queueSize = config.getInt("atlas.persistence.queue-size")

  // Set worker size based on number of cores
  private val writeWorkerSize = getWorkerSize
  logger.info(s"writeWorkerSize=$writeWorkerSize")
  // To expose in admin portal for convenience
  System.setProperty("persistence.writeWorkerSize", writeWorkerSize.toString)

  private val fileConfig = config.getConfig("atlas.persistence.local-file")
  private val dataDir = fileConfig.getString("data-dir")

  private val rollingConf = RollingConfig(
    fileConfig.getLong("max-records"),
    fileConfig.getDuration("max-duration").toMillis,
    fileConfig.getDuration("max-late-duration").toMillis,
    fileConfig.getString("avro-codec"),
    fileConfig.getInt("avro-deflate-compressionLevel"),
    fileConfig.getInt("avro-syncInterval")
  )

  private var queue: SourceQueue[List[Datapoint]] = _
  private var flowComplete: Future[Done] = _

  override def startImpl(): Unit = {
    logger.info("Starting service")
    val (q, f) = StreamOps
      .blockingQueue[List[Datapoint]](registry, "LocalFilePersistService", queueSize)
      .via(balancer(getRollingFileFlow, writeWorkerSize))
      .toMat(Sink.ignore)(Keep.both)
      .run
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
    import scala.concurrent.duration._
    RestartFlow.withBackoff(
      minBackoff = 1.second,
      maxBackoff = 3.seconds,
      randomFactor = 0,
      maxRestarts = -1
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
      import akka.stream.scaladsl.GraphDSL.Implicits._
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

  def persist(dp: List[Datapoint]): Unit = {
    queue.offer(dp)
  }
}
