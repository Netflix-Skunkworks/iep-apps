/*
 * Copyright 2014-2025 Netflix, Inc.
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

import com.netflix.iep.aws2.AwsClientFactory

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.SinkShape
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.util.Optional
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*
import scala.util.Failure
import scala.util.Success

class S3CopySink(
  val awsFactory: AwsClientFactory,
  val s3Config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends GraphStage[SinkShape[File]]
    with StrictLogging {

  private val in = Inlet[File]("S3CopySink.in")
  override val shape: SinkShape[File] = SinkShape(in)

  private val bucket = s3Config.getString("bucket")
  private val region = s3Config.getString("region")
  private val prefix = s3Config.getString("prefix")
  private val maxInactiveMs = s3Config.getDuration("max-inactive-duration").toMillis
  private val clientTimeout = s3Config.getDuration("client-timeout").toScala
  private val threadPoolSize = s3Config.getInt("thread-pool-size")

  private val globalEc = ExecutionContext.global

  private val s3Ec =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threadPoolSize))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {

      private var s3Client: S3Client = _
      private val numActiveFiles = registry.distributionSummary("persistence.s3.numActiveFiles")
      private val numTempFilesToCopy = registry.counter("persistence.s3.tempFilesToCopy")
      // Tracking files which is being processed to avoid duplication
      private val activeFiles = new ConcurrentHashMap[String, Option[KillSwitch]]

      override def preStart(): Unit = {
        initS3Client()
        pull(in)
      }

      override def onPush(): Unit = {
        val file = grab(in)
        if (shouldProcess(file)) {
          process(file)
        }
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        super.completeStage()
        activeFiles.values.asScala.foreach(option => option.foreach(_.shutdown()))
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        activeFiles.values.asScala.foreach(option => option.foreach(_.shutdown()))
        super.failStage(ex)
      }

      setHandler(in, this)

      private def initS3Client(): Unit = {
        s3Client =
          awsFactory.getInstance("s3", classOf[S3Client], null, Optional.of(Region.of(region)))
      }

      def shouldProcess(f: File): Boolean = {
        S3CopyUtils.shouldProcess(
          file = f,
          activeFiles = activeFiles.keySet().asScala.toSet,
          maxInactiveMs = maxInactiveMs,
          isTmpFile = FileUtil.isTmpFile
        )
      }

      // Start a new stream to copy each file
      private def process(file: File): Unit = {
        // Add to map to mark file being processed
        activeFiles.put(file.getName, None)

        if (FileUtil.isTmpFile(file)) {
          numTempFilesToCopy.increment()
        }

        val (killSwitch, streamFuture) = Source
          .future(Future[Unit](copyToS3Sync(file))(s3Ec))
          // Set a timeout for s3 async client call because it sometimes never completes and current
          // file cannot be re-processed because it's been marked in activeFiles already
          .completionTimeout(clientTimeout)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.foreach(_ => cleanupFile(file)))(Keep.both)
          .run()

        // Remove file to allow this file to be re-processed in case the flow fail to process
        streamFuture.onComplete {
          case Success(_) =>
            activeFiles.remove(file.getName)
            logger.debug(s"Stream done for file $file")
          case Failure(te: TimeoutException) =>
            activeFiles.remove(file.getName)
            logger.error(s"Stream timeout for file $file: ${te.getMessage}")
          case Failure(e) =>
            activeFiles.remove(file.getName)
            logger.error(s"Stream failed for file $file", e)
        }(globalEc)

        // Use computeIfPresent to makes sure only add if it's not been removed, in theory there's a
        // chance the flow has completed before it reach here
        activeFiles.computeIfPresent(file.getName, (_, _) => Option(killSwitch))

        numActiveFiles.record(activeFiles.size)
      }

      def cleanupFile(file: File): Unit = {
        FileUtil.delete(file)
        activeFiles.remove(file.getName)
      }

      private def copyToS3Sync(file: File): Unit = {
        val s3Key = buildS3Key(file.getName)
        logger.debug(s"copyToS3 start: file=$file, key=$s3Key")
        val putReq = PutObjectRequest
          .builder()
          .bucket(bucket)
          .key(s3Key)
          .build()
        s3Client.putObject(putReq, file.toPath)
        logger.info(s"copyToS3 done: file=$file, key=$s3Key")
      }

      private def buildS3Key(fileName: String): String =
        S3CopyUtils.buildS3Key(fileName, prefix, HourlyRollingWriter.HourStringLen)
    }
  }
}

object S3CopySink {

  /**
    * Extract start and end minute from file name's suffix, which is start and end seconds of hour.
    * For example a file name "xyz.1200-1320" would be extracted as "20-22".
    * Use the special range "61-61" for tmp files because they don't have time range suffix.
    */
  def extractMinuteRange(fileName: String): String = {
    if (FileUtil.isTmpFile(fileName)) {
      "61-61"
    } else {
      val len = fileName.length
      val startMinute = fileName.substring(len - 9, len - 5).toInt / 60
      val endMinute = fileName.substring(len - 4).toInt / 60
      "%02d".format(startMinute) + "-" + "%02d".format(endMinute)
    }
  }
}
