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
package com.netflix.atlas.aggregator

import tools.jackson.core.StreamReadFeature
import tools.jackson.core.StreamWriteFeature
import tools.jackson.dataformat.smile.SmileFactory
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.pekko.CustomMediaTypes
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.pekko.ThreadPools
import com.netflix.spectator.api.Registry
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.HttpEncodings
import org.apache.pekko.http.scaladsl.model.headers.`Content-Encoding`
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.RestartFlow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString

import java.io.ByteArrayOutputStream
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class PekkoClient(registry: Registry, implicit val system: ActorSystem) {

  import PekkoClient.*

  private val encodingParallelism = math.max(2, Runtime.getRuntime.availableProcessors() / 2)

  private val ec: ExecutionContext =
    ThreadPools.fixedSize(registry, "PublishEncoding", encodingParallelism)

  def createPublisherQueue(
    name: String,
    queueSize: Int
  ): StreamOps.BlockingSourceQueue[RequestTuple] = {
    val blockingQueue = new ArrayBlockingQueue[RequestTuple](queueSize)
    StreamOps
      .wrapBlockingQueue[RequestTuple](registry, name, blockingQueue)
      .via(createPublisherFlow)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  def createPublisherFlow: Flow[RequestTuple, Unit, NotUsed] = {
    val flow = Flow[RequestTuple]
      .mapAsyncUnordered(encodingParallelism) { t =>
        Future(t.mkRequest() -> t)(ec)
      }
      .via(Http().superPool[RequestTuple]())
      .map {
        case (response, t) =>
          t.complete(response)
          response.foreach(_.discardEntityBytes())
      }
    val restartSettings = RestartSettings(5.seconds, 5.seconds, 1.0)
      .withLogSettings(RestartSettings.LogSettings(Attributes.LogLevels.Warning))
    RestartFlow.onFailuresWithBackoff(restartSettings) { () =>
      flow
    }
  }
}

object PekkoClient {

  val VoidInstance: Void = null.asInstanceOf[Void]

  private val headers = List(`Content-Encoding`(HttpEncodings.gzip))

  class RequestTuple(uri: Uri, id: String, payload: AnyRef, encode: AnyRef => ByteString) {

    private val promise: Promise[Void] = Promise()
    private var logger: AccessLogger = _

    def mkRequest(): HttpRequest = {
      val entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, encode(payload))
      val request = HttpRequest(HttpMethods.POST, uri, headers, entity)
      logger = AccessLogger.newClientLogger(id, request)
      request
    }

    def complete(result: Try[HttpResponse]): Unit = {
      logger.complete(result)
      result match {
        case Success(_) => promise.success(VoidInstance)
        case Failure(e) => promise.failure(e)
      }
    }

    def future: CompletableFuture[Void] = {
      import scala.jdk.FutureConverters.*
      promise.future.asJava.toCompletableFuture
    }
  }

  val factory: SmileFactory = SmileFactory
    .builder()
    .enable(StreamReadFeature.AUTO_CLOSE_SOURCE)
    .enable(StreamWriteFeature.AUTO_CLOSE_TARGET)
    .build()

  private val streams = new ThreadLocal[ByteArrayOutputStream]

  /** Use thread local to reuse byte array buffers across calls. */
  def getOrCreateStream: ByteArrayOutputStream = {
    var baos = streams.get
    if (baos == null) {
      baos = new ByteArrayOutputStream
      streams.set(baos)
    } else {
      baos.reset()
    }
    baos
  }
}
