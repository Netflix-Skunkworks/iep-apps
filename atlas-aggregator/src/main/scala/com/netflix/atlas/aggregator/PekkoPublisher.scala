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

import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.util.ByteString
import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.core.util.FastGzipOutputStream
import com.netflix.spectator.api.Measurement
import com.netflix.spectator.atlas.Publisher
import com.netflix.spectator.atlas.impl.EvalPayload
import com.netflix.spectator.atlas.impl.PublishPayload

import java.util.concurrent.CompletableFuture
import scala.util.Using

/**
  * Use Pekko-Http client for sending data instead of default which is based on the
  * URLConnection class built into the JDK. This helps reduce the number of threads
  * needed overall.
  */
class PekkoPublisher(config: AggrConfig, client: PekkoClient) extends Publisher {

  import PekkoClient.*
  import PekkoPublisher.*

  private val atlasUri = Uri(config.uri())
  private val evalUri = Uri(config.evalUri())

  private val pubConfig = config.config.getConfig("atlas.aggregator.publisher")
  private val queueSize = pubConfig.getInt("queue-size")

  private val atlasClient = client.createPublisherQueue("publisher-atlas", queueSize)
  private val lwcClient = client.createPublisherQueue("publisher-lwc", queueSize)

  override def init(): Unit = {}

  override def publish(payload: PublishPayload): CompletableFuture[Void] = {
    val t = new RequestTuple(atlasUri, "publisher-atlas", payload, encode)
    if (atlasClient.offer(t))
      CompletableFuture.completedFuture(VoidInstance)
    else
      CompletableFuture.failedFuture(new IllegalStateException("failed to enqueue atlas request"))
  }

  override def publish(payload: EvalPayload): CompletableFuture[Void] = {
    val t = new RequestTuple(evalUri, "publisher-lwc", payload, encode)
    if (lwcClient.offer(t))
      CompletableFuture.completedFuture(VoidInstance)
    else
      CompletableFuture.failedFuture(new IllegalStateException("failed to enqueue lwc request"))
  }

  override def close(): Unit = {
    atlasClient.complete()
    lwcClient.complete()
  }
}

object PekkoPublisher {

  /** Encode publish payload to a byte array. */
  private def encode(payload: AnyRef): ByteString = {
    val baos = PekkoClient.getOrCreateStream
    Using.resource(new FastGzipOutputStream(baos)) { out =>
      Using.resource(PekkoClient.factory.createGenerator(out)) { gen =>
        payload match {
          case p: PublishPayload => encode(gen, p)
          case p: EvalPayload    => encode(gen, p)
          case p =>
            throw new IllegalArgumentException(s"unknown payload type: ${p.getClass.getName}")
        }
      }
    }
    ByteString.fromArrayUnsafe(baos.toByteArray)
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
}
