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
package com.netflix.iep.lwc

import java.util.concurrent.atomic.AtomicLong
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.RestartFlow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.pekko.ByteStringInputStream
import com.netflix.atlas.json3.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.netflix.spectator.atlas.impl.Subscriptions
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.InputStream
import java.util.zip.GZIPInputStream
import scala.concurrent.Future
import scala.util.Success
import scala.util.Using

/**
  * Refresh the set of expressions from the LWC service.
  */
class ExprUpdateService(
  config: Config,
  registry: Registry,
  evaluator: ExpressionsEvaluator,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  import scala.concurrent.duration.*
  import com.netflix.atlas.pekko.OpportunisticEC.*

  private val configUri = Uri(config.getString("netflix.iep.lwc.bridge.config-uri"))

  private val lastUpdateTime = PolledMeter
    .using(registry)
    .withName("lwc.expressionsAge")
    .monitorValue(new AtomicLong(registry.clock().wallTime()), Functions.age(registry.clock()))

  @volatile private var responseEtag = ""

  private val syncPayloadBytes = registry.distributionSummary("lwc.syncPayloadBytes")
  private val syncPayloadExprs = registry.distributionSummary("lwc.syncPayloadExprs")

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    val request = HttpRequest(HttpMethods.GET, configUri)
    val client = Http().superPool[AccessLogger]()
    val flow = Flow[HttpRequest]
      .map { req =>
        val etag = responseEtag
        if (etag.isEmpty) {
          req -> AccessLogger.newClientLogger("lwc-subs", req)
        } else {
          val r = req.addHeader(`If-None-Match`(EntityTag(etag)))
          r -> AccessLogger.newClientLogger("lwc-subs", r)
        }
      }
      .via(client)
      .map {
        case (result, accessLog) =>
          accessLog.complete(result)
          result
      }
      .collect {
        case Success(response) => response
      }
      .via(syncExpressionsFlow)
    val restartSettings = RestartSettings(5.seconds, 5.seconds, 1.0)
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings) { () =>
      flow
    }
    killSwitch = Source
      .tick(0.seconds, 10.seconds, request)
      .via(restartFlow)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  private[lwc] def syncExpressionsFlow: Flow[HttpResponse, NotUsed, NotUsed] = {
    Flow[HttpResponse]
      .flatMapConcat {
        case response if response.status == StatusCodes.OK =>
          val etag = response.headers
            .collectFirst {
              case ETag(entityTag) => entityTag.tag
            }
            .getOrElse("")
          response.entity
            .withoutSizeLimit()
            .dataBytes
            .reduce(_ ++ _)
            .map { bytes =>
              syncPayloadBytes.record(bytes.size)
              etag -> bytes
            }
        case response =>
          if (response.status == StatusCodes.NotModified) {
            logger.debug("no modification to subscriptions")
            lastUpdateTime.set(registry.clock().wallTime())
          }
          response.discardEntityBytes()
          Source.empty
      }
      .buffer(1, OverflowStrategy.dropHead)
      .flatMapConcat {
        case (etag, data) => Source.future(update(etag, data))
      }
  }

  private def update(etag: String, data: ByteString): Future[NotUsed] = {
    import scala.jdk.CollectionConverters.*
    Future {
      try {
        Using.resource(ExprUpdateService.inputStream(data)) { in =>
          val exprs = Json
            .decode[Subscriptions](in)
            .getExpressions
            .asScala
            .filter(_.getFrequency == 60000) // Limit to the primary publish step size
            .asJava
          evaluator.sync(exprs)
          syncPayloadExprs.record(exprs.size())
          lastUpdateTime.set(registry.clock().wallTime())
          responseEtag = etag
        }
      } catch {
        case e: Exception =>
          logger.error("failed to parse and sync expressions", e)
      }
      NotUsed
    }
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }
}

object ExprUpdateService {

  // Magic header to recognize GZIP compressed data
  // http://www.zlib.org/rfc-gzip.html#file-format
  private val gzipMagicHeader = ByteString(Array(0x1F.toByte, 0x8B.toByte))

  /**
    * Create an InputStream for reading the content of the ByteString. If the data is
    * gzip compressed, then it will be wrapped in a GZIPInputStream to handle the
    * decompression of the data. This can be handled at the server layer, but it may
    * be preferable to decompress while parsing into the final object model to reduce
    * the need to allocate an intermediate ByteString of the uncompressed data.
    */
  private def inputStream(bytes: ByteString): InputStream = {
    if (bytes.startsWith(gzipMagicHeader))
      new GZIPInputStream(new ByteStringInputStream(bytes))
    else
      new ByteStringInputStream(bytes)
  }
}
