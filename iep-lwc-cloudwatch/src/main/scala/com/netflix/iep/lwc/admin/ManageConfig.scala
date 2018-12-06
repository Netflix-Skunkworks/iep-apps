/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.iep.lwc.admin

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.client.RequestBuilding.Put
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object ManageConfig extends App with StrictLogging {

  val schema = JsonSchemaFactory
    .byDefault()
    .getJsonSchema(
      Json.decode[JsonNode](
        scala.io.Source.fromResource("cluster-config-schema.json").reader()
      )
    )

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import CustomEventStreamUnmarshalling._
  import system.dispatcher

  val configBinHost = sys.env("CONFIGBIN_HOST")
  val clustersConfigSyncUrl = s"http://$configBinHost${sys.env("CLUSTERS_CONFIG_SYNC_PATH")}"
  val clusterConfigsUrl = s"http://$configBinHost${sys.env("CLUSTERS_CONFIG_PATH")}"

  def configBinSource(url: String): Future[Source[ServerSentEvent, NotUsed]] = {
    Http()
      .singleRequest(
        Get(url)
      )
      .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
  }

  def configs(): Flow[ServerSentEvent, Data, NotUsed] = {
    Flow[ServerSentEvent]
      .map(e => Json.decode[Data](e.data))
      .takeWhile(_.eventType != "done")
  }

  def validate(): Flow[Data, (Data, Boolean, Iterable[String]), NotUsed] = {
    Flow[Data]
      .map { data =>
        val report = schema.validate(data.data.payload)
        (data, report.isSuccess(), report.asScala.map(_.getMessage))
      }
  }

  def validateAllConfigs(): Unit = {
    configBinSource(clustersConfigSyncUrl)
      .foreach { source =>
        source
          .via(configs())
          .via(validate())
          .filter {
            case (_, valid, _) =>
              !valid
          }
          .map {
            case (data, _, message) =>
              logger.info(s"${data.key} $message")
          }
          .runWith(Sink.ignore)
          .onComplete { result =>
            result match {
              case Success(_) =>
                logger.info("Completed")
              case Failure(e) =>
                logger.error("Failure", e)
            }
            system.terminate()
          }
      }
  }

  def fixConfigs(matchError: Iterable[String] => Boolean, fix: Data => Data) = {
    configBinSource(clustersConfigSyncUrl)
      .foreach { source =>
        source
          .via(configs())
          .via(validate())
          .filter {
            case (_, valid, _) =>
              !valid
          }
          .map {
            case (data, _, messages) =>
              if (matchError(messages)) {
                Some(fix(data))
              } else {
                None
              }
          }
          .flatMapConcat(c => Source(c.toList))
          .map { data =>
            (
              Put(
                s"$clusterConfigsUrl/${data.key}",
                Json.encode(data.data)
              ),
              data.key
            )
          }
          .via(Http().cachedHostConnectionPool[String](configBinHost))
          .map {
            case (Success(response), key) =>
              logger.info(s"Uploading config $key was successful: ${response.status}")
              response.discardEntityBytes()
            case (Failure(ex), key) =>
              logger.error(s"Uploading $key failed with $ex")
          }
          .runWith(Sink.ignore)
          .onComplete { result =>
            result match {
              case Success(_) =>
                logger.info("Completed")
              case Failure(e) =>
                logger.error("Failure", e)
            }
            system.terminate()
          }
      }

  }

  def setEmail(data: Data): Data = {
    val v = data.data.version.deepCopy()
    v.set("comment", TextNode.valueOf("Set email address"))
    v.set("user", TextNode.valueOf("fooUser"))

    val p = data.data.payload.deepCopy()
    p.set("email", TextNode.valueOf("fooUser@netflix.com"))

    val d = data.data.copy(version = v, payload = p)
    data.copy(data = d)
  }

  validateAllConfigs()

//  fixConfigs(
//    _.exists(
//      _ == "object has missing required properties ([\"email\"])"
//    ),
//    setEmail(_)
//  )

}

case class Data(
  @JsonProperty("type")
  eventType: String,
  key: String,
  data: Content
)

case class Content(
  version: ObjectNode,
  payload: ObjectNode
)

object CustomEventStreamUnmarshalling extends CustomEventStreamUnmarshalling

trait CustomEventStreamUnmarshalling extends EventStreamUnmarshalling {
  override def maxEventSize: Int = super.maxEventSize * 2
  override def maxLineSize: Int = super.maxLineSize * 3
}
