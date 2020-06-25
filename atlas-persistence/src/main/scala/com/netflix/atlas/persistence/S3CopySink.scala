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
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.SinkShape
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.util.Failure
import scala.util.Success

class S3CopySink(
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

  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private val globalEc = ExecutionContext.global
  private val s3Ec =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threadPoolSize))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {

      private var s3Client: S3Client = _
      private val numActiveFiles = registry.distributionSummary("persistence.s3.numActiveFiles")
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

      // TODO use iep/iep-module-aws2
      private def initS3Client(): Unit = {
        s3Client = S3Client
          .builder()
          .region(Region.of(region))
          .build()
      }

      def shouldProcess(f: File): Boolean = {
        if (activeFiles.containsKey(f.getName)) {
          logger.debug(s"Should NOT process: being processed - $f")
          false
        } else if (FileUtil.isTmpFile(f)) {
          if (isInactive(f)) {
            logger.warn(s"Should process: temp file but inactive - $f")
            true
          } else {
            false
          }
        } else {
          true
        }
      }

      private def isInactive(f: File): Boolean = {
        val lastModified = f.lastModified()
        if (lastModified == 0) {
          false // Error getting lastModified, assuming not inactive yet
        } else {
          System.currentTimeMillis > f.lastModified() + maxInactiveMs
        }
      }

      // Start a new stream to copy each file
      private def process(file: File): Unit = {
        // Add to map to mark file being processed
        activeFiles.put(file.getName, None)

        val (killSwitch, streamFuture) = Source
          .fromFuture(Future[Unit](copyToS3Sync(file))(s3Ec))
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
        activeFiles.computeIfPresent(file.getName, (k, v) => Option(killSwitch))

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

      // Example file name: 2020-05-10T0300.i-localhost.1.XkvU3A
      private def buildS3Key(fileName: String): String = {
        val hour = fileName.substring(0, HourlyRollingWriter.HourStringLen)
        val s3FileName = fileName.substring(HourlyRollingWriter.HourStringLen + 1)
        val hourPath = hash(s"$prefix/$hour")
        s"$hourPath/$s3FileName"
      }

      private def hash(path: String): String = {
        val md = MessageDigest.getInstance("MD5")
        md.update(path.getBytes("UTF-8"))
        val digest = md.digest()
        val hexBytes = digest.take(2).map("%02x".format(_)).mkString
        val randomPrefix = hexBytes.take(3)
        s"$randomPrefix/$path"
      }
    }
  }
}
