package com.netflix.atlas.persistence

import java.io.File
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.SinkShape
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RestartSource
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse

import scala.collection.mutable

class S3CopySink(implicit val system: ActorSystem)
    extends GraphStage[SinkShape[File]]
    with StrictLogging {

  private val in = Inlet[File]("S3CopySink.in")
  override val shape = SinkShape(in)

  private implicit val ec = scala.concurrent.ExecutionContext.global // TODO custom pool?
  private implicit val mat = ActorMaterializer()

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {

      private val s3Bucket = "atlas.us-east-1.ieptest.netflix.net" //TODO config + expand
      private val region = Region.US_EAST_1 //TODO dynamic
      private val s3Prefix = "hourly-raw-avro" //TODO temp prefix

      private var s3Client: S3AsyncClient = _
      //TODO custom client config
      private val s3ClientConfig = ClientOverrideConfiguration
        .builder()
        .build()

      // TODO use this map to track active files
      private val activeFiles = mutable.Map[String, KillSwitch]()

      override def preStart(): Unit = {
        initS3Client()
        pull(in)
      }

      override def onPush(): Unit = {
        val file = grab(in)
        if (shouldCopy(file)) {
          process(file)
        }
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        super.completeStage()
        activeFiles.values.foreach(_.shutdown())
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.failStage(ex)
        activeFiles.values.foreach(_.shutdown())
      }

      setHandler(in, this)

      private def initS3Client(): Unit = {
        s3Client = S3AsyncClient
          .builder()
          .region(region)
          .overrideConfiguration(s3ClientConfig)
          .build()
      }

      // TODO handle .tmp after long idle?
      private def shouldCopy(f: File): Boolean = {
        !f.getName.endsWith(".tmp")
      }

      // Start a new stream to copy each file
      private def process(file: File): Unit = {

        import scala.concurrent.duration._
        import scala.jdk.FutureConverters._

        val killSwitch = RestartSource
          .onFailuresWithBackoff(
            minBackoff = 1.second,
            maxBackoff = 5.seconds,
            randomFactor = 0,
            maxRestarts = Int.MaxValue
          ) { () =>
            Source.fromFuture(copyToS3Async(file).asScala)
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.foreach(_ => FileUtil.delete(file.toPath, logger)))(Keep.left)
          .run
      }

      private def copyToS3Async(file: File): CompletableFuture[PutObjectResponse] = {
        val s3Key = buildS3Key(file.getName)
        val putReq = PutObjectRequest
          .builder()
          .bucket(s3Bucket)
          .key(s3Key)
          .build()

        logger.debug(s"copyToS3 start: file=$file, key=$s3Key")
        s3Client.putObject(putReq, file.toPath).whenComplete { (res, err) =>
          {
            if (res != null) {
              logger.debug(s"copyToS3 done: file=$file, key=$s3Key")
            } else {
              logger.error(s"copyToS3 failed: file=$file, key=$s3Key, error=${err.getMessage}")
            }
          }
        }
      }

      // Example file name: 2020051003.i-localhost.1.XkvU3A
      private def buildS3Key(fileName: String): String = {
        val nameWithSlash = fileName.replaceFirst("\\.", "/")
        //TODO hash it, use common prefix for now for easier test data cleanup
        s"$s3Prefix/$nameWithSlash"
      }

      private def hash(path: String): String = {
        val md = MessageDigest.getInstance("MD5")
        md.update(path.getBytes("UTF-8"))
        val digest = md.digest()
        val hexBytes = digest.take(2).map("%02x".format(_)).mkString
        val prefix = hexBytes.take(3)
        prefix + (if (path(0) == '/') path else "/" + path)
      }
    }
  }
}
