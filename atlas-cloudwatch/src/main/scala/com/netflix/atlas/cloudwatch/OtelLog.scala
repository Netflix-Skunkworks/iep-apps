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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.json3.Json
import com.typesafe.scalalogging.StrictLogging

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.net.Socket
import scala.concurrent.duration.*

final case class OtelLog(
  logts: Long,
  message: String,
  level: String,
  logger: String,
  tags: Map[String, Any]
)

object OtelTcpLogger extends StrictLogging {

  private val DefaultHost = "localhost"
  private val DefaultPort = 1552

  private val host: String =
    sys.env.getOrElse("OTEL_HOST", DefaultHost)

  private val port: Int =
    sys.env.get("OTEL_PORT").flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(DefaultPort)

  private val connectTimeout: FiniteDuration = 500.millis
  private val maxAttempts: Int = 5
  private val retryDelay: FiniteDuration = 1.second

  def buildLog(
    message: String,
    level: String,
    loggerName: String,
    tags: Map[String, Any]
  ): OtelLog = {
    val ts = System.currentTimeMillis()
    OtelLog(
      logts = ts,
      message = message,
      level = level,
      logger = loggerName,
      tags = tags
    )
  }

  // Serialize all logs up-front so JSON encoding never happens inside the socket write.
  private def trySendBatch(lines: Seq[String], attempt: Int): Boolean = {
    var socket: Socket = null
    try {
      socket = new Socket()
      socket.connect(new InetSocketAddress(host, port), connectTimeout.toMillis.toInt)
      val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream, "UTF-8"))
      lines.foreach(writer.write)
      writer.flush()
      true
    } catch {
      case e: Exception =>
        logger.warn(
          s"[otel-tcp-logger] attempt $attempt: error sending batch of ${lines.size}: ${e.getMessage}",
          e
        )
        false
    } finally {
      if (socket != null) {
        try socket.close()
        catch {
          case t: Throwable =>
            logger.warn(
              s"[otel-tcp-logger] attempt $attempt: error closing socket: ${t.getMessage}",
              t
            )
        }
      }
    }
  }

  /**
   * Send all logs in a single TCP connection — one socket open/write/close per batch
   * instead of one per log. Retries the whole batch on connection or write failure.
   */
  def sendBatch(logs: Seq[OtelLog]): Unit = {
    val lines = logs.map(log => Json.encode(log) + "\n")

    var attempt = 1
    var done = false
    while (attempt <= maxAttempts && !done) {
      if (trySendBatch(lines, attempt)) {
        done = true
      } else if (attempt < maxAttempts) {
        Thread.sleep(retryDelay.toMillis)
        attempt += 1
      } else {
        logger.warn(s"[otel-tcp-logger] giving up after $maxAttempts attempts")
        attempt += 1
      }
    }
  }
}

object OtelTcpSink extends OtelLogSink {

  override def sendBatch(logs: Seq[OtelLog]): Unit = OtelTcpLogger.sendBatch(logs)
}
