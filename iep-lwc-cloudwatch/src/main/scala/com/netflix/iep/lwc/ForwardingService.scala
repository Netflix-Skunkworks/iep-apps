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

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Framing
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.netflix.atlas.pekko.AccessLogger
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.lwc.fwd.cw.*
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse

import java.time.Instant
import java.util.function.ToDoubleFunction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ForwardingService(
  config: Config,
  registry: Registry,
  configStats: ConfigStats,
  evaluator: Evaluator,
  clientFactory: AwsClientFactory,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  import ForwardingService.*

  private val streamFailures = registry.counter("forwarding.streamFailures")

  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val clock = registry.clock()

  private val lastSuccessfulPutTime = PolledMeter
    .using(registry)
    .withName("forwarding.timeSinceLastPut")
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.age(clock))

  private val numConfigs = PolledMeter
    .using(registry)
    .withName("forwarding.numConfigs")
    .monitorValue(new AtomicLong())

  private val numDataSources = PolledMeter
    .using(registry)
    .withName("forwarding.numDataSources")
    .monitorValue(new AtomicLong())

  PolledMeter
    .using(registry)
    .withName("forwarding.staleDataSources")
    .withDelay(java.time.Duration.ofMinutes(1))
    .monitorValue(configStats, (_.staleExpressions): ToDoubleFunction[ConfigStats])

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    val putEnabled = config.getBoolean("iep.lwc.cloudwatch.put-enabled")
    val pattern = Pattern.compile(config.getString("iep.lwc.cloudwatch.filter"))
    val uri = config.getString("iep.lwc.cloudwatch.uri")
    val namespace = config.getString("iep.lwc.cloudwatch.namespace")
    val adminUri = Uri(config.getString("iep.lwc.cloudwatch.admin-uri"))
    val client = Http().superPool[AccessLogger]()

    def put(
      region: String,
      account: String,
      request: PutMetricDataRequest
    ): Future[PutMetricDataResponse] = {
      import scala.jdk.FutureConverters.*
      if (putEnabled) {
        val cwClient = clientFactory.getInstance(region, classOf[CloudWatchAsyncClient], account)
        cwClient.putMetricData(request).asScala
      } else {
        Future.successful(PutMetricDataResponse.builder().build())
      }
    }

    killSwitch = autoReconnectHttpSource(uri, client)
      .via(configInput(registry))
      .map { configs =>
        numConfigs.set(configs.size)
        val filtered = configs.map {
          case (key, config) =>
            val exprs = config.expressions.filter(e => pattern.matcher(e.atlasUri).matches())
            key -> config.copy(expressions = exprs)
        }
        configStats.updateConfigs(filtered)
        filtered
      }
      .via(toDataSources)
      .map { dss =>
        numDataSources.set(dss.sources().size())
        dss
      }
      .via(evaluator.createStreamsFlow)
      .via(toMetricDatum(config, registry, configStats))
      .via(sendToCloudWatch(lastSuccessfulPutTime, namespace, put))
      .via(sendToAdmin(adminUri, client))
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

  type PutFunction = (String, String, PutMetricDataRequest) => Future[PutMetricDataResponse]

  type Client = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]

  private val MaxFrameLength = 65536

  def autoReconnectHttpSource(uri: String, client: Client): Source[ByteString, NotUsed] = {
    Source
      .repeat(NotUsed)
      .throttle(1, 1.second, 1, ThrottleMode.Shaping)
      .flatMapConcat(_ => httpSource(uri, client))
  }

  def httpSource(uri: String, client: Client): Source[ByteString, NotUsed] = {
    Source
      .single(HttpRequest(HttpMethods.GET, uri))
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
        r.entity
          .withoutSizeLimit()
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
    val done = registry.counter(baseId.withTag("id", "done"))
    val start = registry.counter(baseId.withTag("id", "start"))
    val repoVersion = registry.gauge("forwarding.repoVersion")

    val clock = registry.clock()
    val lastHeartbeat = PolledMeter
      .using(registry)
      .withName("forwarding.timeSinceLastHeartbeat")
      .monitorValue(new AtomicLong(clock.wallTime()), Functions.age(clock))

    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), MaxFrameLength, allowTruncation = true))
      .map(_.decodeString(StandardCharsets.UTF_8).trim)
      .filter(_.nonEmpty)
      .map { s =>
        val msg = Message(s)
        logger.debug(s"message [${msg.str}]")

        msg match {
          case m if m.isHeartbeat =>
            heartbeats.increment()
            lastHeartbeat.set(clock.wallTime())
            repoVersion.set(m.repoVersion.toDouble)
          case m if m.isInvalid => invalid.increment()
          case m if m.isUpdate  => (if (m.response.isDelete) deletes else updates).increment()
          case m if m.isStart   => start.increment()
          case m if m.isDone    => done.increment()
          case _                =>
        }

        msg
      }
      .via(new ConfigManager)
  }

  def toDataSources: Flow[Map[String, ClusterConfig], Evaluator.DataSources, NotUsed] = {
    Flow[Map[String, ClusterConfig]]
      .map { configs =>
        import scala.jdk.CollectionConverters.*
        val exprs = configs.flatMap {
          case (key, config) => config.expressions.map(toDataSource(_, key))
        }
        new Evaluator.DataSources(exprs.toSet.asJava)
      }
  }

  def toDataSource(expression: ForwardingExpression, key: String): Evaluator.DataSource = {
    val id = Json.encode(ExpressionId(key, expression))
    new Evaluator.DataSource(id, expression.atlasUri)
  }

  def toMetricDatum(
    config: Config,
    registry: Registry,
    configStats: ConfigStats
  ): Flow[Evaluator.MessageEnvelope, ForwardingMsgEnvelope, NotUsed] = {
    val datapoints = registry.counter("forwarding.cloudWatchDatapoints")

    Flow[Evaluator.MessageEnvelope]
      .filter { env =>
        env.message() match {
          case ts: TimeSeriesMessage =>
            logger.trace(s"time series message: ${ts.toJson}")
            datapoints.increment()
            true
          case other: JsonSupport =>
            logger.debug(s"diagnostic message: ${other.toJson}")
            false
        }
      }
      .map { env =>
        val id = Json.decode[ExpressionId](env.id())
        val msg = env.message().asInstanceOf[TimeSeriesMessage]
        msg.data match {
          case d: ArrayData if !d.values(0).isNaN =>
            configStats.updateStats(id.key, id.expression.atlasUri, msg.end)
          case _ =>
          // Ignore if data is NaN
        }
        ForwardingMsgEnvelope(id, createMetricDatum(config, id.expression, msg), None)
      }
  }

  def createMetricDatum(
    config: Config,
    expression: ForwardingExpression,
    msg: TimeSeriesMessage
  ): Option[AccountDatum] = {
    import scala.jdk.CollectionConverters.*
    val name = Strings.substitute(expression.metricName, msg.tags)
    val accountId = Strings.substitute(expression.account, msg.tags)

    val dfltRegionStr = config.getString("netflix.iep.aws.region")
    val regionStr =
      Strings.substitute(expression.region.getOrElse(dfltRegionStr), msg.tags)

    // CloudWatch only supports 1 or 60, 1 being high resolution.
    val resolution = if (msg.step >= 60_000) 60 else 1

    msg.data match {
      case data: ArrayData if !data.values(0).isNaN =>
        // For Atlas, the timestamp indicates the end of the interval, for CW the corresponding
        // time would be one step interval earlier.
        val timestamp = Instant.ofEpochMilli(msg.start - msg.step)
        val datum = MetricDatum
          .builder()
          .metricName(name)
          .dimensions(
            expression.dimensions.map(toDimension(_, msg.tags)).asJava
          )
          .timestamp(timestamp)
          .value(truncate(data.values(0)))
          .storageResolution(resolution)
          .build()
        Some(AccountDatum(regionStr, accountId, datum))
      case _ =>
        None
    }

  }

  def toDimension(dimension: ForwardingDimension, tags: Map[String, String]): Dimension = {
    Dimension
      .builder()
      .name(dimension.name)
      .value(Strings.substitute(dimension.value, tags))
      .build()
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
    doPut: PutFunction
  ): Flow[ForwardingMsgEnvelope, ForwardingMsgEnvelope, NotUsed] = {

    // groupedWithin based on CloudWatch limits of 1000 metrics per put
    // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
    import scala.jdk.CollectionConverters.*
    Flow[ForwardingMsgEnvelope]
      .groupBy(
        Int.MaxValue,
        d => s"${d.accountDatum.map(_.region)}.${d.accountDatum.map(_.account)}"
      ) // one client per region/account or no client when accountDatum is empty
      .groupedWithin(1000, 5.seconds)
      .flatMapMerge(
        Int.MaxValue,
        { data =>
          if (data.head.accountDatum.isDefined) {
            val request = PutMetricDataRequest
              .builder()
              .namespace(namespace)
              .metricData(data.map(_.accountDatum.get.datum).asJava)
              .build()

            val region = data.head.accountDatum.get.region
            val account = data.head.accountDatum.get.account
            val future = doPut(region, account, request)
            Source
              .future(future)
              .map { _ =>
                logger.whenDebugEnabled {
                  val context = s"(region=$region, account=$account, request=$request)"
                  logger.debug(s"cloudwatch put succeeded $context")
                }
                lastSuccessfulPutTime.set(System.currentTimeMillis())
                data
              }
              .recover {
                case t: Throwable =>
                  val exprs = Json.encode(data.map(_.id))
                  val context =
                    s"(region=$region, account=$account, request=$request, exprs=$exprs)"
                  logger.warn(s"cloudwatch request failed $context", t)
                  data.map(_.copy(error = Some(t)))
              }
              .mapConcat(identity)
          } else {
            Source(data)
          }
        }
      )
      .mergeSubstreams
  }

  def sendToAdmin(adminUri: Uri, client: Client): Flow[ForwardingMsgEnvelope, NotUsed, NotUsed] = {

    Flow[ForwardingMsgEnvelope]
      .groupedWithin(100, 30.seconds)
      .map { msgs =>
        val request =
          HttpRequest(HttpMethods.POST, adminUri, entity = Json.encode(msgs.map(toReport)))
        request -> AccessLogger.newClientLogger("admin", request)
      }
      .via(client)
      .via(StreamOps.map { (t, mat) =>
        val (result, accessLog) = t
        accessLog.complete(result)
        result match {
          case Success(response) =>
            response.discardEntityBytes()(mat)
          case Failure(e) =>
            logger.warn("Error posting report to Admin", e)
        }
        NotUsed
      })
  }

  def toReport(msg: ForwardingMsgEnvelope): Report = {
    import scala.jdk.CollectionConverters.*

    Report(
      System.currentTimeMillis(),
      msg.id,
      msg.accountDatum.map(a =>
        FwdMetricInfo(
          a.region,
          a.account,
          a.datum.metricName(),
          a.datum.dimensions().asScala.map(d => (d.name(), d.value())).toMap
        )
      ),
      msg.error
    )
  }

  //
  // Model objects for configs
  //

  case class Message(str: String) {

    private val data = {
      try {
        Json.decode[Data](str.substring("data:".length))
      } catch {
        case e: Exception =>
          logger.warn(s"failed to parse message from configbin: [$str]", e)
          Data("invalid", null, Json.decode[ObjectNode]("{}"))
      }
    }

    private def isNullOrEmpty(s: String): Boolean = s == null || s.isEmpty

    def isInvalid: Boolean =
      !isDone && (isNullOrEmpty(data.key) || !data.data.fieldNames().hasNext)

    def isStart: Boolean = data.dataType == "start"

    def isDone: Boolean = data.dataType == "done"

    def isHeartbeat: Boolean = data.dataType == "heartbeat"

    def isUpdate: Boolean = data.dataType == "config" && !isInvalid

    def cluster: String = data.key

    def response: ConfigBinResponse = {
      Json.decode[ConfigBinResponse](data.data)
    }

    def repoVersion: Long = {
      val version = data.data.get("clientRepoVersion")
      if (version == null) -1L else version.asLong(-1L)
    }
  }

  case class Data(
    @JsonProperty("type") dataType: String,
    key: String,
    data: ObjectNode
  )

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

  case class ForwardingMsgEnvelope(
    id: ExpressionId,
    accountDatum: Option[AccountDatum],
    error: Option[Throwable]
  )

  //
  // Model objects for pairing CloudWatch objects with account
  //

  case class AccountDatum(region: String, account: String, datum: MetricDatum)

  case class AccountRequest(region: String, account: String, request: PutMetricDataRequest)
}
