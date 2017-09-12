/*
 * Copyright 2014-2017 Netflix, Inc.
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

import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.regex.Pattern
import javax.inject.Inject

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.Dimension
import com.amazonaws.services.cloudwatch.model.MetricDatum
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.aws.Pagination
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Try

class ForwardingService @Inject()(
  config: Config,
  registry: Registry,
  evaluator: Evaluator,
  cwClient: AmazonCloudWatch,
  implicit val system: ActorSystem)

  extends AbstractService {

  import ForwardingService._

  private implicit val mat = ActorMaterializer()

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    val pattern = Pattern.compile(config.getString("iep.lwc.cloudwatch.filter"))
    val uri = config.getString("iep.lwc.cloudwatch.uri")
    val client = Http().superPool[AccessLogger]()
    killSwitch = httpSource(uri, client)
      .via(configInput(registry))
      .via(toDataSources(pattern))
      .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
      .via(toMetricDatum(registry))
      .via(toCloudWatchPut)
      .via(sendToCloudWatch(cwClient))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }

}

object ForwardingService extends StrictLogging {

  //
  // Helpers for constructing parts of the stream
  //

  type Client = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]

  private val MaxFrameLength = 65536

  def httpSource(uri: String, client: Client): Source[ByteString, NotUsed] = {
    val request = HttpRequest(HttpMethods.GET, uri)
    Source.repeat(request)
      .throttle(1, 1.second, 1, ThrottleMode.Shaping)
      .map(r => r -> AccessLogger.newClientLogger("configbin", r))
      .via(client)
      .map {
        case (result, accessLog) =>
          accessLog.complete(result)
          result
      }
      .filter(_.isSuccess)
      .flatMapConcat(r => Source(r.toOption.toList))
      .filter(_.status == StatusCodes.OK)
      .flatMapConcat(_.entity.withoutSizeLimit().dataBytes)
      .recover {
        case t: Throwable =>
          logger.warn("configbin stream failed", t)
          ByteString.empty
      }
  }

  def configInput(registry: Registry): Flow[ByteString, Map[String, ClusterConfig], NotUsed] = {
    val baseId = registry.createId("forwarding.configMessages")
    val heartbeats = registry.counter(baseId.withTag("id", "heartbeat"))
    val invalid = registry.counter(baseId.withTag("id", "invalid"))
    val updates = registry.counter(baseId.withTag("id", "update"))
    val deletes = registry.counter(baseId.withTag("id", "delete"))

    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), MaxFrameLength, allowTruncation = true))
      .map(_.decodeString(StandardCharsets.UTF_8))
      .map(s => Message(s))
      .filter { msg =>
        msg match {
          case m if m.isHeartbeat => logger.debug(m.responseString); heartbeats.increment()
          case m if m.isInvalid   => invalid.increment()
          case m if m.isUpdate    => (if (m.response.isDelete) deletes else updates).increment()
        }
        msg.isUpdate
      }
      .via(new ConfigManager)
  }

  def toDataSources(pattern: Pattern): Flow[Map[String, ClusterConfig], Evaluator.DataSources, NotUsed] = {
    Flow[Map[String, ClusterConfig]]
      .map { configs =>
        import scala.collection.JavaConverters._
        val exprs = configs.values.flatMap { config =>
          config.expressions
            .filter(e => pattern.matcher(e.atlasUri).matches())
            .map(_.toDataSource)
        }
        new Evaluator.DataSources(exprs.toSet.asJava)
      }
  }

  def toMetricDatum(registry: Registry): Flow[Evaluator.MessageEnvelope, MetricDatum, NotUsed] = {
    val datapoints = registry.counter("forwarding.cloudWatchDatapoints")

    Flow[Evaluator.MessageEnvelope]
      .filter { env =>
        env.getMessage match {
          case ts: TimeSeriesMessage =>
            datapoints.increment()
            true
          case other: JsonSupport =>
            logger.debug(s"diagnostic message: ${other.toJson}")
            false
        }
      }
      .map { env =>
        val expr = Json.decode[ForwardingExpression](env.getId)
        val msg = env.getMessage.asInstanceOf[TimeSeriesMessage]
        expr.createMetricDatum(msg)
      }
  }

  def toCloudWatchPut: Flow[MetricDatum, PutMetricDataRequest, NotUsed] = {
    import scala.collection.JavaConverters._
    Flow[MetricDatum]
      .groupedWithin(20, 5.seconds)
      .map { data =>
        new PutMetricDataRequest()
          .withNamespace("NFLX/EPIC")
          .withMetricData(data.asJava)
      }
  }

  def sendToCloudWatch(cwClient: AmazonCloudWatch): Flow[PutMetricDataRequest, NotUsed, NotUsed] = {
    Flow[PutMetricDataRequest]
      .flatMapConcat { request =>
        val pub = Pagination.createPublisher(request, r => cwClient.putMetricData(r))
        Source.fromPublisher(pub)
      }
      .map { response =>
        logger.debug(s"cloudwatch put result: $response")
        NotUsed
      }
      .recover {
        case t: Throwable =>
          logger.warn("cloudwatch request failed", t)
          NotUsed
      }
  }

  //
  // Model objects for configs
  //

  case class Message(str: String) {
    val (cluster: String, responseString: String) = {
      val pos = str.indexOf("->")
      if (pos < 0)
        null.asInstanceOf[String] -> null.asInstanceOf[String]
      else
        str.substring(0, pos) -> str.substring(pos + 2)
    }

    private def isNullOrEmpty(s: String): Boolean = s == null || s.isEmpty

    def isInvalid: Boolean = isNullOrEmpty(cluster) || isNullOrEmpty(responseString)

    def isHeartbeat: Boolean = cluster == "heartbeat"

    def isUpdate: Boolean = !(isInvalid || isHeartbeat)

    def response: ConfigBinResponse = {
      Json.decode[ConfigBinResponse](responseString)
    }
  }

  case class ConfigBinResponse(version: ConfigBinVersion, payload: JsonNode) {
    def isUpdate: Boolean = !isDelete
    def isDelete: Boolean = payload.isTextual && payload.asText().isEmpty

    def clusterConfig: ClusterConfig = {
      require(isUpdate, "cannot retrieve config from a delete response")
      Json.decode[ClusterConfig](payload.toString)
    }
  }

  object ConfigBinResponse {
    def apply(version: ConfigBinVersion, payload: ClusterConfig): ConfigBinResponse = {
      val data = Json.decode[JsonNode](Json.encode(payload))
      apply(version, data)
    }

    def delete(version: ConfigBinVersion): ConfigBinResponse = {
      apply(version, new TextNode(""))
    }
  }

  case class ConfigBinVersion(
    ts: Long,
    hash: String,
    user: Option[String] = None,
    comment: Option[String] = None)

  case class ClusterConfig(email: String, expressions: List[ForwardingExpression])

  case class ForwardingExpression(
    atlasUri: String,
    account: String,
    metricName: String,
    dimensions: List[ForwardingDimension] = Nil) {

    require(atlasUri != null, "atlasUri cannot be null")
    require(account != null, "account cannot be null")
    require(metricName != null, "metricName cannot be null")

    def toDataSource: Evaluator.DataSource = {
      val id = Json.encode(this)
      new Evaluator.DataSource(id, atlasUri)
    }

    def createMetricDatum(msg: TimeSeriesMessage): MetricDatum = {
      import scala.collection.JavaConverters._
      // TODO: account handling
      val name = Strings.substitute(metricName, msg.tags)

      val value = msg.data match {
        case data: ArrayData => data.values(0)
        case _               => Double.NaN
      }

      new MetricDatum()
        .withMetricName(name)
        .withDimensions(dimensions.map(_.toDimension(msg.tags)).asJava)
        .withTimestamp(new Date(msg.start))
        .withValue(value)
    }
  }

  case class ForwardingDimension(name: String, value: String) {
    def toDimension(tags: Map[String, String]): Dimension = {
      new Dimension()
        .withName(name)
        .withValue(Strings.substitute(value, tags))
    }
  }
}
