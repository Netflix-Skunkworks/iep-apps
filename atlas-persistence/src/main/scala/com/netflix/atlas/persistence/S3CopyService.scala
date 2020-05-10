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
import java.nio.file.Path
import java.security.MessageDigest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.concurrent.duration._

class S3CopyService @Inject()(
  val config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  private val baseDir = config.getString("atlas.persistence.local-file.data-dir")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  // TODO bucket name based on region
  private val s3Bucket = "atlas.us-east-1.ieptest.netflix.net"
  private var s3Client: S3Client = _

  override def startImpl(): Unit = {
    logger.info("Starting service")
    s3Client = buildS3Client()
    Source
      .repeat(NotUsed)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      .flatMapMerge(Int.MaxValue, _ => Source(listAllFiles()))
      .filter(shouldCopy)
      .runForeach(copyToS3) // TODO implement a stage or sink to avoid dup copy
  }

  override def stopImpl(): Unit = {
    if (s3Client != null) s3Client.close()
  }

  private def listAllFiles(): List[File] = {
    try {
      val dir = new File(baseDir)
      dir.listFiles().filter(_.isFile).toList
    } catch {
      case e: Exception =>
        logger.error(s"Error listing dir $baseDir", e)
        Nil
    }
  }

  private def shouldCopy(f: File): Boolean = {
    !f.getName.endsWith(".tmp")
  }

  private def copyToS3(f: File): Unit = {
    val s3Key = buildKey("hourly-raw-avro", f.getName) //TODO temp prefix
    logger.debug(s"copy file ${f.getName} to s3 with key: $s3Key")
    copy(s3Key, f.toPath)
    f.delete()
  }

  def buildS3Client(): S3Client = {
    S3Client
      .builder()
      .region(Region.US_EAST_1)    //TODO config
      .build()
  }

  private def copy(key: String, path: Path): Unit = {

    val putReq = PutObjectRequest
      .builder()
      .bucket(s3Bucket)
      .key(key)
      .build()

    val putRes = s3Client.putObject(putReq, path)
    logger.info(s"S3 put response: ${putRes}")
  }

  def buildKey(prefix: String, name: String): String = {
    // Example file name: 2020051003.i-localhost.1.XkvU3A
    val nameWithInsId = name.replaceFirst("\\.", "/")
    s"$prefix/$nameWithInsId"   //TODO hash it, keep it in same prefix during dev test
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
