/*
 * Copyright 2014-2021 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers._
import akka.stream.Attributes
import akka.stream.RestartSettings
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RestartFlow
import akka.stream.scaladsl.Sink
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.core.StreamWriteFeature
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.CustomMediaTypes
import com.netflix.atlas.akka.StreamOps
import com.netflix.spectator.api.Measurement
import com.netflix.spectator.atlas.Publisher
import com.netflix.spectator.atlas.impl.EvalPayload
import com.netflix.spectator.atlas.impl.PublishPayload

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.concurrent.CompletableFuture
import java.util.zip.Deflater
import java.util.zip.GZIPOutputStream
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

/**
  * Use Akka-Http client for sending data instead of default which is based on the
  * URLConnection class built into the JDK. This helps reduce the number of threads
  * needed overall.
  */
class AkkaPublisher(config: AggrConfig, implicit val system: ActorSystem) extends Publisher {

  import AkkaPublisher._

  private implicit val ec: ExecutionContext = system.dispatcher

  private val atlasUri = Uri(config.uri())
  private val evalUri = Uri(config.evalUri())

  private val encodingParallelism = math.max(2, Runtime.getRuntime.availableProcessors() / 2)

  private val client = {
    val flow = Flow[RequestTuple]
      .mapAsync(encodingParallelism) { t =>
        Future(t.mkRequest() -> t)
      }
      .via(Http().superPool[RequestTuple]())
      .map {
        case (response, t) => t.complete(response)
      }
    val restartSettings = RestartSettings(5.seconds, 5.seconds, 1.0)
      .withLogSettings(RestartSettings.LogSettings(Attributes.LogLevels.Warning))
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings) { () =>
      flow
    }
    StreamOps
      .blockingQueue[RequestTuple](config.debugRegistry(), "publisher", 1000)
      .via(restartFlow)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  override def init(): Unit = {}

  private def doPost(uri: Uri, id: String, payload: AnyRef): CompletableFuture[Void] = {
    val t = new RequestTuple(uri, id, payload)
    client.offer(t)
    t.future
  }

  override def publish(payload: PublishPayload): CompletableFuture[Void] = {
    doPost(atlasUri, "publisher-atlas", payload)
  }

  override def publish(payload: EvalPayload): CompletableFuture[Void] = {
    doPost(evalUri, "publisher-lwc", payload)
  }

  override def close(): Unit = {
    client.complete()
  }
}

object AkkaPublisher {

  private val VoidInstance = null.asInstanceOf[Void]

  private val headers = List(`Content-Encoding`(HttpEncodings.gzip))

  class RequestTuple(uri: Uri, id: String, payload: AnyRef) {

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
      import scala.jdk.FutureConverters._
      promise.future.asJava.toCompletableFuture
    }
  }

  private val factory = SmileFactory
    .builder()
    .enable(StreamReadFeature.AUTO_CLOSE_SOURCE)
    .enable(StreamWriteFeature.AUTO_CLOSE_TARGET)
    .build()

  private val streams = new ThreadLocal[ByteArrayOutputStream]

  /** Use thread local to reuse byte array buffers across calls. */
  private def getOrCreateStream: ByteArrayOutputStream = {
    var baos = streams.get
    if (baos == null) {
      baos = new ByteArrayOutputStream
      streams.set(baos)
    } else {
      baos.reset()
    }
    baos
  }

  /** Encode publish payload to a byte array. */
  private def encode(payload: AnyRef): Array[Byte] = {
    val baos = getOrCreateStream
    Using.resource(new GzipLevelOutputStream(baos)) { out =>
      Using.resource(factory.createGenerator(out)) { gen =>
        payload match {
          case p: PublishPayload => encode(gen, p)
          case p: EvalPayload    => encode(gen, p)
          case p =>
            throw new IllegalArgumentException("unknown payload type: " + p.getClass.getName)
        }
      }
    }
    baos.toByteArray
  }

  private def encode(gen: JsonGenerator, payload: PublishPayload): Unit = {
    // Common tags are always empty for this use-case and can be omitted
    gen.writeStartObject()
    gen.writeArrayFieldStart("metrics")
    val metrics = payload.getMetrics
    val n = metrics.size()
    var i = 0
    while (i < n) {
      encode(gen, metrics.get(i))
      i += 1
    }
    gen.writeEndArray()
    gen.writeEndObject()
  }

  private def encode(gen: JsonGenerator, m: Measurement): Unit = {
    val id = m.id
    gen.writeStartObject()
    gen.writeObjectFieldStart("tags")
    gen.writeStringField("name", id.name)
    val n = id.size
    var i = 1
    while (i < n) {
      val k = id.getKey(i)
      val v = id.getValue(i)
      gen.writeStringField(k, v)
      i += 1
    }
    gen.writeEndObject()
    gen.writeNumberField("timestamp", m.timestamp)
    gen.writeNumberField("value", m.value)
    gen.writeEndObject()
  }

  private def encode(gen: JsonGenerator, payload: EvalPayload): Unit = {
    gen.writeStartObject()
    gen.writeNumberField("timestamp", payload.getTimestamp)
    gen.writeArrayFieldStart("metrics")
    encodeMetrics(gen, payload.getMetrics)
    gen.writeEndArray()
    gen.writeArrayFieldStart("messages")
    encodeMessages(gen, payload.getMessages)
    gen.writeEndArray()
    gen.writeEndObject()
  }

  private def encodeMetrics(
    gen: JsonGenerator,
    metrics: java.util.List[EvalPayload.Metric]
  ): Unit = {
    val n = metrics.size()
    var i = 0
    while (i < n) {
      val m = metrics.get(i)
      gen.writeStartObject()
      gen.writeStringField("id", m.getId)
      gen.writeObjectFieldStart("tags")
      m.getTags.forEach((k, v) => gen.writeStringField(k, v))
      gen.writeEndObject()
      gen.writeNumberField("value", m.getValue)
      gen.writeEndObject()
      i += 1
    }
  }

  private def encodeMessages(
    gen: JsonGenerator,
    msgs: java.util.List[EvalPayload.Message]
  ): Unit = {
    val n = msgs.size()
    var i = 0
    while (i < n) {
      val m = msgs.get(i)
      gen.writeStartObject()
      gen.writeStringField("id", m.getId)
      gen.writeObjectFieldStart("message")
      gen.writeStringField("type", m.getMessage.getType.name())
      gen.writeStringField("message", m.getMessage.getMessage)
      gen.writeEndObject()
      gen.writeEndObject()
      i += 1
    }
  }

  /** Wrap GZIPOutputStream to set the best speed compression level. */
  final class GzipLevelOutputStream(out: OutputStream) extends GZIPOutputStream(out) {
    `def`.setLevel(Deflater.BEST_SPEED)
  }
}
