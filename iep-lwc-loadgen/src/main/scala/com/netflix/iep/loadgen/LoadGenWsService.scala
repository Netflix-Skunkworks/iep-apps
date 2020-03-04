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
package com.netflix.iep.loadgen

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.model.ws.WebSocketRequest
import akka.stream.AbruptTerminationException
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

class LoadGenWsService @Inject()(
  config: Config,
  registry: Registry,
  evaluator: Evaluator,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  import LoadGenService._

  private val streamFailures = registry.counter("loadgen.streamFailures")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  private var killSwitch: KillSwitch = _

  private val atlasStreamWsUri = config.getString("iep.lwc.loadgen.atlasStreamWsUri")
  private val uriStart = config.getInt("iep.lwc.loadgen.uri.start")
  private val uriEnd = config.getInt("iep.lwc.loadgen.uri.end")
  private val numMsgsCounter = registry.counter("loadgen.numMessages")
  private val shouldLogResult = config.getBoolean("iep.lwc.loadgen.shouldLogResult")

  override def startImpl(): Unit = {
    killSwitch = Source
      .repeat(dataSources)
      .throttle(1, 1.minute, 1, ThrottleMode.Shaping)
      .via(evalSourceWsFlow)
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) | Failure(_: AbruptTerminationException) =>
            // AbruptTerminationException will be triggered if the associated ActorSystem
            // is shutdown before the stream.
            logger.info(s"shutting down LoadGenWsService")
          case Failure(t) =>
            streamFailures.increment()
            logger.error(s"LoadGenWsService failed, attempting to restart", t)
            startImpl()
        }
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(updateStats))(Keep.left)
      .run()
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }

  private def evalSourceWsFlow: Flow[Evaluator.DataSources, String, NotUsed] = {
    Flow[Evaluator.DataSources]
      .map(dss => {
        TextMessage(Json.encode(dss.getSources))
      })
      .via(
        Http.get(system).webSocketClientFlow(WebSocketRequest(atlasStreamWsUri))
      )
      .flatMapConcat {
        case msg: TextMessage =>
          msg.textStream.fold("")(_ + _).map(ByteString(_))
        case _: BinaryMessage =>
          // Should not happen, count times of occurrences in case it happenbs
          logger.warn("received BinaryMessage")
          Source.single(
            ByteString(
              """{"id":"_","message":{"type":"_BinaryMessage_","message":"heartbeat"}}"""
            )
          )
      }
      .map(_.utf8String)
      .mapMaterializedValue(_ => NotUsed)
  }

  private def dataSources: Evaluator.DataSources = {
    import scala.collection.JavaConverters._
    val defaultStep = config.getDuration("iep.lwc.loadgen.step")
    val uris = config
      .getStringList("iep.lwc.loadgen.uris")
      .asScala
      .zipWithIndex
      .map {
        case (uri, i) =>
          val step = extractStep(uri).getOrElse(defaultStep)
          val id = Strings.zeroPad(i, 6)
          new Evaluator.DataSource(id, step, uri)
      }
      .slice(uriStart, uriEnd)

    new Evaluator.DataSources(uris.toSet.asJava)
  }

  private def updateStats(envelope: String): Unit = {
    try {
      numMsgsCounter.increment()

      if (shouldLogResult) {
        logger.info(s"datapoint: $envelope")
      }

      val dataNode = Json.decode[JsonNode](envelope)
      if (envelope.contains("\"type\":\"error\"")) {
        logger.error(s"got error message: $envelope")
      }

      dataNode.get("message") match {
        case node: JsonNode if (node.has("data")) => record("timeseries")
        case node: JsonNode if (node.has("type")) => record(node.get("type").asText)
        case _                                    => record("unknown")
      }
    } catch {
      case e: Exception => logger.error("error processing message: " + envelope, e)
    }
  }

  private def record(msgType: String): Unit = {
    val resultMessages = registry
      .createId("loadgen.resultMessages")
      .withTags("msgType", msgType)
    registry.counter(resultMessages).increment()
  }
}
