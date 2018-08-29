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
package com.netflix.iep.lwc

import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import javax.inject.Inject
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.AbruptTerminationException
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
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.NetflixEnvironment
import com.netflix.iep.aws.AwsClientFactory
import com.netflix.iep.aws.Pagination
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ForwardingService @Inject()(
  config: Config,
  registry: Registry,
  evaluator: Evaluator,
  clientFactory: AwsClientFactory,
  implicit val system: ActorSystem)

  extends AbstractService with StrictLogging {

  import ForwardingService._

  private val streamFailures = registry.counter("forwarding.streamFailures")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  private val clock = registry.clock()
  private val lastSuccessfulPutTime = PolledMeter
    .using(registry)
    .withName("forwarding.timeSinceLastPut")
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.age(clock))

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    val pattern = Pattern.compile(config.getString("iep.lwc.cloudwatch.filter"))
    val uri = config.getString("iep.lwc.cloudwatch.uri")
    val namespace = config.getString("iep.lwc.cloudwatch.namespace")
    val client = Http().superPool[AccessLogger]()
    def put(region: String, account: String, request: PutMetricDataRequest): PutMetricDataResult = {
      val cwClient = clientFactory.getInstance(region, classOf[AmazonCloudWatch], account)
      cwClient.putMetricData(request)
    }
    killSwitch = autoReconnectHttpSource(uri, client)
      .via(configInput(registry))
      .via(toDataSources(pattern))
      .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
      .via(toMetricDatum(registry))
      .via(sendToCloudWatch(lastSuccessfulPutTime, namespace, put))
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) | Failure(_: AbruptTerminationException) =>
            // AbruptTerminationException will be triggered if the associated ActorSystem
            // is shutdown before the stream.
            logger.info(s"shutting down forwarding stream")
          case Failure(t) =>
            streamFailures.increment()
            logger.error(s"forwarding stream failed, attempting to restart", t)
            startImpl()
        }
      }
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
  // Constants for interacting with CloudWatch. For more details see:
  //
  // http://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
  // https://github.com/Netflix/servo/blob/master/servo-aws/src/test/java/com/netflix/servo/publish/cloudwatch/CloudWatchValueTest.java
  //

  /**
    * Experimentally derived value for the largest exponent that can be sent to cloudwatch
    * without triggering an InvalidParameterValue exception. See CloudWatchValueTest for the test
    * program that was used.
    */
  private val MaxExponent = 360

  /**
    * Experimentally derived value for the largest exponent that can be sent to cloudwatch
    * without triggering an InvalidParameterValue exception. See CloudWatchValueTest for the test
    * program that was used.
    */
  private val MinExponent = -360

  /** Maximum value that can be represented in cloudwatch. */
  private val MaxValue = math.pow(2.0, MaxExponent)

  //
  // Helpers for constructing parts of the stream
  //

  type PutFunction = (String, String, PutMetricDataRequest) => PutMetricDataResult

  type Client = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]

  private val MaxFrameLength = 65536

  def autoReconnectHttpSource(uri: String, client: Client): Source[ByteString, NotUsed] = {
    Source.repeat(NotUsed)
      .throttle(1, 1.second, 1, ThrottleMode.Shaping)
      .flatMapConcat(_ => httpSource(uri, client))
  }

  def httpSource(uri: String, client: Client): Source[ByteString, NotUsed] = {
    Source.single(HttpRequest(HttpMethods.GET, uri))
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
      .flatMapConcat { r =>
        r.entity.withoutSizeLimit()
          .dataBytes
          .recover {
            case t: Throwable =>
              logger.warn("configbin stream failed", t)
              ByteString.empty
          }
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

  def toMetricDatum(registry: Registry): Flow[Evaluator.MessageEnvelope, AccountDatum, NotUsed] = {
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

  /**
    * Adjust a double value so it can be successfully written to cloudwatch. This involves capping
    * values with large exponents to an experimentally determined max value and converting values
    * with large negative exponents to 0. In addition, NaN values will be converted to 0.
    */
  def truncate(number: Number): Double = {
    val value = number.doubleValue()
    val exponent = Math.getExponent(value)
    value match {
      case v if v.isNaN                            => 0.0
      case v if exponent >= MaxExponent && v < 0.0 => -MaxValue
      case v if exponent >= MaxExponent && v > 0.0 => MaxValue
      case _ if exponent <= MinExponent            => 0.0
      case v                                       => v
    }
  }

  /**
    * Batch the input datapoints and send to CloudWatch.
    *
    * @param lastSuccessfulPutTime
    *     Gets updated with the current timestamp everytime there is a successful put. Can
    *     be used to track if there is a failure and we are not successfully sending any data
    *     to cloudwatch.
    * @param namespace
    *     Namespace to use for the custom metrics sent to CloudWatch. For more information
    *     see: http://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/aws-namespaces.html.
    * @param doPut
    *     Put the metric data into cloudwatch. This is typically a reference to the Amazon
    *     client.
    * @return
    *     Flow converting a stream of MetricDatum to a stream of PutMetricDataRequest objects.
    */
  def sendToCloudWatch(
    lastSuccessfulPutTime: AtomicLong,
    namespace: String,
    doPut: PutFunction): Flow[AccountDatum, NotUsed, NotUsed] = {

    import scala.collection.JavaConverters._
    Flow[AccountDatum]
      .groupBy(Int.MaxValue, d => s"${d.region}.${d.account}") // one client per region/account
      .groupedWithin(20, 5.seconds)
      .flatMapConcat { data =>
        val request = new PutMetricDataRequest()
          .withNamespace(namespace)
          .withMetricData(data.map(_.datum).asJava)

        val region = data.head.region
        val account = data.head.account
        val pub = Pagination.createPublisher(
          request, (r: PutMetricDataRequest) => doPut(region, account, r))
        Source.fromPublisher(pub)
          .map { response =>
            logger.debug(s"cloudwatch put result: $response")
            lastSuccessfulPutTime.set(System.currentTimeMillis())
            NotUsed
          }
          .recover {
            case t: Throwable =>
              logger.warn("cloudwatch request failed", t)
              NotUsed
          }
      }
      .mergeSubstreams
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
    region: Option[String],
    metricName: String,
    dimensions: List[ForwardingDimension] = Nil) {

    require(atlasUri != null, "atlasUri cannot be null")
    require(account != null, "account cannot be null")
    require(metricName != null, "metricName cannot be null")

    def toDataSource: Evaluator.DataSource = {
      val id = Json.encode(this)
      new Evaluator.DataSource(id, atlasUri)
    }

    def createMetricDatum(msg: TimeSeriesMessage): AccountDatum = {
      import scala.collection.JavaConverters._
      val name = Strings.substitute(metricName, msg.tags)
      val accountId = Strings.substitute(account, msg.tags)

      val regionStr = Strings.substitute(region.getOrElse(NetflixEnvironment.region()), msg.tags)

      val value = msg.data match {
        case data: ArrayData => data.values(0)
        case _               => Double.NaN
      }

      val datum = new MetricDatum()
        .withMetricName(name)
        .withDimensions(dimensions.map(_.toDimension(msg.tags)).asJava)
        .withTimestamp(new Date(msg.start))
        .withValue(truncate(value))

      AccountDatum(regionStr, accountId, datum)
    }
  }

  case class ForwardingDimension(name: String, value: String) {
    def toDimension(tags: Map[String, String]): Dimension = {
      new Dimension()
        .withName(name)
        .withValue(Strings.substitute(value, tags))
    }
  }

  //
  // Model objects for pairing CloudWatch objects with account
  //

  case class AccountDatum(region: String, account: String, datum: MetricDatum)

  case class AccountRequest(region: String, account: String, request: PutMetricDataRequest)
}
