/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.webapi

import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import com.netflix.atlas.json.Json
import com.netflix.atlas.webapi.RequestId.getByteArrayOutputStream

import java.io.ByteArrayOutputStream
import scala.util.Using

/**
  * This is the class we have to use to respond to the Firehose publisher. The request ID and timestamp must match
  * those of the message received in order for Firehose to consider the data successfully processed.
  *
  * @param requestId
  *     The request ID given by AWS Firehose.
  * @param timestamp
  *     The timestamp in unix epoch milliseconds given by AWS Firehose.
  * @param exception
  *     An optional exception to encode if processing failed. This will be stored in Cloud Watch logs.
  */
case class RequestId(requestId: String, timestamp: Long, exception: Option[Exception] = None) {

  def getEntity: HttpEntity.Strict = {
    val stream = getByteArrayOutputStream
    Using.resource(Json.newJsonGenerator(stream)) { json =>
      json.writeStartObject()
      json.writeStringField("requestId", requestId)
      json.writeNumberField("timestamp", timestamp)
      exception.map { ex =>
        json.writeStringField("errorMessage", ex.getMessage)
      }
      json.writeEndObject()
    }
    HttpEntity(ContentTypes.`application/json`, stream.toByteArray)
  }

}

object RequestId {

  private val byteArrayStreams = new ThreadLocal[ByteArrayOutputStream]

  private[atlas] def getByteArrayOutputStream: ByteArrayOutputStream = {
    var baos = byteArrayStreams.get()
    if (baos == null) {
      baos = new ByteArrayOutputStream()
      byteArrayStreams.set(baos)
    } else {
      baos.reset()
    }
    baos
  }
}
