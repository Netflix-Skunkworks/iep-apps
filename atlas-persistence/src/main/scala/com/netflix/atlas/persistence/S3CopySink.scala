package com.netflix.atlas.persistence

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest

import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.SinkShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.collection.mutable

class S3CopySink extends GraphStage[SinkShape[File]] with StrictLogging {
  private val in = Inlet[File]("S3CopySink.in")
  override val shape = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {

      private val s3Bucket = "atlas.us-east-1.ieptest.netflix.net" //TODO config + expand
      private val region = Region.US_EAST_1 //TODO dynamic
      private val s3Prefix = "hourly-raw-avro" //TODO temp prefix

      //TODO restart S3 client as needed, or at least reload credentials with timer?
      private var s3Client: S3AsyncClient = _

      private val fileToFlow = mutable.Map[String, Any]()

      override def preStart(): Unit = {
        initS3Client()
        pull(in)
      }

      override def onPush(): Unit = {
        process(grab(in))
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        super.completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.failStage(ex)
      }

      setHandler(in, this)

      // TODO client config - timeout retry ...
      def initS3Client(): Unit = {
        s3Client = S3AsyncClient
          .builder()
          .region(region)
          .build()
      }

      private def process(file: File): Unit = {

        val s3Key = buildS3Key(file.getName)

        logger.debug(s"copyToS3 start: file=$file, key=$s3Key")

        val putReq = PutObjectRequest
          .builder()
          .bucket(s3Bucket)
          .key(buildS3Key(file.getName))
          .build()

        val path = file.toPath

        s3Client.putObject(putReq, path).whenComplete { (res, err) =>
          {
            if (res != null) {
              logger.debug(s"copyToS3 done: file=$file, key=$s3Key")
              try {
                Files.delete(path)
              } catch { case e: Exception => logger.error(s"failed to delete file: $path", e) }
            } else {
              logger.error(s"copyToS3 FAILED: file=$file, key=$s3Key, error=${err.getMessage}")
              //TODO Retry
            }
          }
        }
      }

      // Example file name: 2020051003.i-localhost.1.XkvU3A
      private def buildS3Key(fileName: String): String = {
        val nameWithSlash = fileName.replaceFirst("\\.", "/")
        s"$s3Prefix/$nameWithSlash" //TODO hash it, keep it in same prefix for dev
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
