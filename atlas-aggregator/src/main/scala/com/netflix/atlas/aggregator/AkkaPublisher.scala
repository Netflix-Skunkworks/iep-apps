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
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers._
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.CustomMediaTypes
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

  private val headers = List(`Content-Encoding`(HttpEncodings.gzip))

  override def init(): Unit = {}

  private def encode(payload: AnyRef): Array[Byte] = {
    val baos = getOrCreateStream
    Using.resource(new GzipLevelOutputStream(baos)) { out =>
      mapper.writeValue(out, payload)
    }
    baos.toByteArray
  }

  private def doPost(uri: Uri, id: String, payload: AnyRef): CompletableFuture[Void] = {
    import scala.jdk.FutureConverters._
    val entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, encode(payload))
    val request = HttpRequest(HttpMethods.POST, uri, headers, entity)
    val logger = AccessLogger.newClientLogger(id, request)
    val future = Http().singleRequest(request)
    future.onComplete(logger.complete)
    future.map(_ => VoidInstance).asJava.toCompletableFuture
  }

  override def publish(payload: PublishPayload): CompletableFuture[Void] = {
    doPost(atlasUri, "publisher-atlas", payload)
  }

  override def publish(payload: EvalPayload): CompletableFuture[Void] = {
    doPost(evalUri, "publisher-lwc", payload)
  }

  override def close(): Unit = {}
}

object AkkaPublisher {

  private val VoidInstance = null.asInstanceOf[Void]

  private val mapper = {
    val serializer = new JsonSerializer[Measurement] {
      override def serialize(
        value: Measurement,
        gen: JsonGenerator,
        serializers: SerializerProvider
      ): Unit = {
        val id = value.id
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
        gen.writeNumberField("timestamp", value.timestamp)
        gen.writeNumberField("value", value.value)
        gen.writeEndObject()
      }
    }
    val module = new SimpleModule().addSerializer(classOf[Measurement], serializer)
    new ObjectMapper(new SmileFactory()).registerModule(module)
  }

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

  /** Wrap GZIPOutputStream to set the best speed compression level. */
  final class GzipLevelOutputStream(out: OutputStream) extends GZIPOutputStream(out) {
    `def`.setLevel(Deflater.BEST_SPEED)
  }
}
