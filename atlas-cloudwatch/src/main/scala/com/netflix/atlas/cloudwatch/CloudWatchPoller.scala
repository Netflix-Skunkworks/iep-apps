/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toTagMap
import com.netflix.atlas.cloudwatch.CloudWatchPoller.runAfter
import com.netflix.atlas.cloudwatch.CloudWatchPoller.runKey
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest
import software.amazon.awssdk.services.cloudwatch.model.Metric

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.StreamConverters.StreamHasToScala
import scala.util.Failure
import scala.util.Success

/**
  * Poller for CloudWatch metrics. It is meant to handle daily or infrequently posted CloudWatch metrics
  * that are not available via Firehose. For example, the S3 bucket aggregates.
  *
  * Schedules are organized by offsets for ONLY daily metrics at this point. The schedule runs
  * frequently and checks the last polled timestamp to determine if polling should occur. Actual
  * calls to CloudWatch will only happen once a day to keep polling costs down.
  *
  * TODO - Add support for other frequencies if we need them.
  * TODO - Pekko/Pekka streamify it. It should be simple but I had a hard time parallelizing
  * the calls and backpressuring properly, particularly with the async AWS client.
  */
class CloudWatchPoller(
  config: Config,
  registry: Registry,
  leaderStatus: LeaderStatus,
  accountSupplier: AwsAccountSupplier,
  rules: CloudWatchRules,
  clientFactory: AwsClientFactory,
  processor: CloudWatchMetricsProcessor,
  debugger: CloudWatchDebugger
)(implicit val system: ActorSystem)
    extends StrictLogging {

  private implicit val executionContext: ExecutionContext =
    system.dispatchers.lookup("aws-poller-io-dispatcher")

  private val pollTime = registry.timer("atlas.cloudwatch.poller.pollTime")
  private val errorSetup = registry.createId("atlas.cloudwatch.poller.failure", "call", "setup")
  private val errorListing = registry.createId("atlas.cloudwatch.poller.failure", "call", "list")
  private val errorStats = registry.createId("atlas.cloudwatch.poller.failure", "call", "metric")
  private val emptyListing = registry.createId("atlas.cloudwatch.poller.emptyList")

  private val globalLastUpdate =
    PolledMeter
      .using(registry)
      .withId(registry.createId("atlas.cloudwatch.poller.lastRun"))
      .monitorValue(
        new AtomicLong(System.currentTimeMillis()),
        Functions.AGE
      )

  private val dpsDroppedTags =
    registry.counter("atlas.cloudwatch.poller.dps.dropped", "reason", "tags")

  private val dpsDroppedFilter =
    registry.counter("atlas.cloudwatch.poller.dps.dropped", "reason", "filter")
  private val dpsExpected = registry.counter("atlas.cloudwatch.poller.dps.expected")
  private val hrmRequest = registry.createId("atlas.cloudwatch.poller.hrm.request")
  private val dpsPolled = registry.counter("atlas.cloudwatch.poller.dps.polled")
  private val frequency = config.getDuration("atlas.cloudwatch.poller.frequency").getSeconds
  private val hrmFrequency = config.getDuration("atlas.cloudwatch.poller.hrmFrequency").getSeconds

  private[cloudwatch] val flagMap = new ConcurrentHashMap[String, AtomicBoolean]()

  private[cloudwatch] val offsetMap = {
    var map = Map.empty[Int, List[MetricCategory]]
    rules
      .getCategories(config)
      .filter(c => c.pollOffset.isDefined)
      .foreach { category =>
        val offset = category.pollOffset.get.getSeconds.toInt
        val categories = map.getOrElse(offset, List.empty)
        map += offset -> (categories :+ category)
        logger.info(
          s"Setting offset of ${offset}s for ns ${category.namespace} period ${category.period}"
        )
      }
    logger.info(s"Loaded ${map.size} polling offsets")
    map
  }

  private[cloudwatch] val fastPollingAccounts =
    config.getStringList("atlas.cloudwatch.account.polling.fastPolling")

  private[cloudwatch] val awsRequestLimit =
    config.getInt("atlas.cloudwatch.account.polling.requestLimit")

  {
    // Scheduling Pollers Based on Offset
    offsetMap.foreach {
      case (offset, categories) =>
        logger.info(s"Scheduling poller for offset ${offset}s")
        val scheduleFrequency = if (offset < 60) hrmFrequency else frequency
        system.scheduler.scheduleAtFixedRate(
          FiniteDuration(scheduleFrequency, TimeUnit.SECONDS),
          FiniteDuration(scheduleFrequency, TimeUnit.SECONDS)
        )(() => poll(offset, categories))(system.dispatcher)
    }
  }

  private[cloudwatch] def poll(
    offset: Int,
    categories: List[MetricCategory],
    fullRunUt: Option[Promise[List[CloudWatchPoller#Poller]]] = None,
    accountsUt: Option[Promise[Done]] = None
  ): Unit = {
    if (!leaderStatus.hasLeadership) {
      logger.info(s"Not the leader for ${offset}s, skipping CloudWatch polling.")
      fullRunUt.map(_.success(List.empty))
      return
    }

    val start = System.currentTimeMillis()
    val futures = List.newBuilder[Future[Done]]
    val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val runners = List.newBuilder[Poller]

    try {
      accountSupplier.accounts.foreach {
        case (account, regions) =>
          regions.foreach {
            case (region, namespaces) =>
              if (namespaces.nonEmpty) {
                val filtered = categories.filter(c => namespaces.contains(c.namespace))
                if (
                  filtered.nonEmpty && shouldPoll(offset, account, region, filtered.head.period)
                ) {
                  val key = runKey(offset, account, region)
                  val flag = flagMap.computeIfAbsent(key, _ => new AtomicBoolean(false))
                  if (flag.compareAndSet(false, true)) {
                    val client = createClient(account, region)
                    val catFutures = categories.map { category =>
                      val runner = Poller(now, category, client, account, region, offset)
                      this.synchronized {
                        runners += runner
                        runner.execute
                      }
                    }
                    futures += Future.reduceLeft(catFutures)((_, _) => Done).andThen {
                      case Success(_) =>
                        handleSuccess(key, timeToRun(filtered.head.period, offset, account, region))
                      case Failure(_) => handleFailure(key)
                    }
                  } else {
                    logger.warn(
                      s"Skipping polling for period ${offset}s ${account} in ${region} as it was already running."
                    )
                  }
                }
              }
          }
      }

      finalizeFutureResponse()
      accountsUt.map(_.success(Done))
    } catch {
      case ex: Exception =>
        logger.error("Unexpected exception polling for CloudWatch data.", ex)
        registry
          .counter(errorSetup.withTags("exception", ex.getClass.getSimpleName))
          .increment()
        accountsUt.map(_.failure(ex))
        fullRunUt.map(_.failure(ex))
    }

    def finalizeFutureResponse(): Unit = {
      Future.sequence(futures.result()).onComplete {
        case Success(_) =>
          var expecting = 0
          var got = 0
          runners.result().foreach { cr =>
            expecting += cr.expecting.get()
            got += cr.got.get()
          }
          dpsExpected.increment(expecting)
          dpsPolled.increment(got)
          logger.info(
            s"Finished CloudWatch polling for ${offset}s with ${got} of ${expecting} metrics in ${(System.currentTimeMillis() - start) / 1000.0}s"
          )
          globalLastUpdate.set(System.currentTimeMillis())
          pollTime.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
          fullRunUt.map(_.success(runners.result()))
        case Failure(ex) =>
          logger.error(
            "Failure at some point in polling for CloudWatch data." +
              " Not updating the next run time so we can retry.",
            ex
          )
          pollTime.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
          fullRunUt.map(_.failure(ex))
      }
    }
  }

  private def shouldPoll(offset: Int, account: String, region: Region, period: Int): Boolean = {
    if (offset <= 60 && !fastPollingAccounts.contains(account)) false
    else timeToRun(period, offset, account, region) > 0
  }

  private def createClient(account: String, region: Region): CloudWatchClient = {
    clientFactory.getInstance(
      s"$account.$region",
      classOf[CloudWatchClient],
      account,
      Optional.of(region)
    )
  }

  private def handleSuccess(key: String, nextRun: Long): Unit = {
    processor.updateLastSuccessfulPoll(key, nextRun)
    getFlag(key).foreach(_.set(false))
  }

  private def handleFailure(key: String): Unit = {
    getFlag(key).foreach(_.set(false))
  }

  private def getFlag(key: String): Option[AtomicBoolean] = {
    Option(flagMap.get(key)).orElse {
      logger.error(s"No flag found for key $key")
      None
    }
  }

  case class Poller(
    now: Instant,
    category: MetricCategory,
    client: CloudWatchClient,
    account: String,
    region: Region,
    offset: Int
  ) {

    private val minCacheEntries = config.getInt("atlas.cloudwatch.min-cache-entries")
    private val nowMillis = now.toEpochMilli
    private[cloudwatch] val expecting = new AtomicInteger()
    private[cloudwatch] val got = new AtomicInteger()

    private[cloudwatch] def execute: Future[Done] = {
      logger.info(
        s"Polling for account ${account} at ${offset}s and ns ${category.namespace} period ${category.period} in ${region}"
      )
      val futures = category.toListRequests.map { tuple =>
        val (definition, request) = tuple
        val promise = Promise[Done]()
        executionContext.execute(ListMetrics(request, definition, promise))
        promise.future
      }

      Future.reduceLeft(futures)((_, _) => Done).andThen {
        case Success(_) =>
          logger.info(
            s"Finished polling with ${got.get()} of ${expecting.get()} for ${account} at ${offset} and ${
                category.namespace
              } in region ${region} in ${(System.currentTimeMillis() - nowMillis) / 1000.0} s"
          )
        case Failure(_) => // it will bubble up.
      }
    }

    private[cloudwatch] case class ListMetrics(
      request: ListMetricsRequest,
      definition: MetricDefinition,
      promise: Promise[Done]
    ) extends Runnable {

      def process(metricsList: List[Metric]): Unit = {
        logger.info(s"CloudWatch listed ${metricsList.size} metrics for ${
            request.metricName()
          } in ${account} and ${category.namespace} ${definition.name} in region ${region}")
        if (metricsList.isEmpty) {
          registry
            .counter(
              emptyListing.withTags(
                "account",
                account,
                "aws.namespace",
                category.namespace,
                "region",
                region.toString
              )
            )
            .increment()
        }
        val futures = new Array[Future[Done]](metricsList.size)
        var idx = 0
        val wtf = Source
          .fromIterator(() => metricsList.iterator)
          .throttle(awsRequestLimit, 1.second)
          .runForeach { metric =>
            if (!category.dimensionsMatch(metric.dimensions().asScala.toList)) {
              debugger.debugPolled(metric, IncomingMatch.DroppedTag, nowMillis, category)
              dpsDroppedTags.increment()
              futures(idx) = Future.successful(Done)
            } else if (category.filter.map(_.matches(toTagMap(metric))).getOrElse(false)) {
              debugger.debugPolled(metric, IncomingMatch.DroppedFilter, nowMillis, category)
              dpsDroppedFilter.increment()
              futures(idx) = Future.successful(Done)
            } else {
              val promise = Promise[Done]()
              futures(idx) = promise.future
              expecting.incrementAndGet()
              executionContext.execute(FetchMetricStats(definition, metric, promise))
            }
            idx += 1
          }

        wtf.onComplete {
          case Success(_) =>
            // completes on an empty metric list as well.
            Future.sequence(futures.toList).onComplete {
              case Success(_) =>
                promise.success(Done)
              case Failure(ex) =>
                logger.error(
                  s"Failed at least one polling for ${account} and ${category.namespace} ${definition.name} in region ${region}",
                  ex
                )
                promise.failure(ex)
            }
          case Failure(ex) => promise.failure(ex)
        }

      }

      @Override def run(): Unit = {
        try {
          val metrics = client.listMetricsPaginator(request)
          process(metrics.metrics().stream().toScala(List))
        } catch {
          case ex: Exception =>
            logger.error(
              s"Error listing metrics for ${account} and ${category.namespace} ${definition.name} in region ${region}",
              ex
            )
            registry
              .counter(errorListing.withTags("exception", ex.getClass.getSimpleName))
              .increment()
            promise.failure(ex)
        }
      }

      private[cloudwatch] def utHack(exp: Int, polled: Int): Unit = {
        expecting.set(exp)
        got.set(polled)
      }
    }

    private[cloudwatch] case class FetchMetricStats(
      definition: MetricDefinition,
      metric: Metric,
      promise: Promise[Done]
    ) extends Runnable {

      @Override def run(): Unit = {
        try {
          val start = now.minusSeconds(minCacheEntries * category.period)
          val request = MetricMetadata(category, definition, metric.dimensions.asScala.toList)
            .toGetRequest(start, now)

          if (request.period() < 60) {
            registry
              .counter(hrmRequest.withTags("period", request.period().toString))
              .increment()
          }
          val response = client.getMetricStatistics(request)
          val dimensions = request.dimensions().asScala.toList.toBuffer
          dimensions += Dimension.builder().name("nf.account").value(account).build()
          dimensions += Dimension.builder().name("nf.region").value(region.toString).build()

          if (response.datapoints().isEmpty) {
            debugger.debugPolled(metric, IncomingMatch.DroppedEmpty, nowMillis, category)
          } else {
            debugger.debugPolled(
              metric,
              IncomingMatch.Accepted,
              nowMillis,
              category,
              response.datapoints()
            )
          }

          response
            .datapoints()
            .asScala
            .map { dp =>
              val m = FirehoseMetric(
                "",
                metric.namespace(),
                metric.metricName(),
                dimensions.toList,
                dp
              )
              if (category.period < 60) {
                processor.sendToRegistry(m, category, nowMillis)
              } else {
                processor.updateCache(m, category, nowMillis)
              }
            }
          got.incrementAndGet()
          promise.success(Done)
        } catch {
          case ex: Exception =>
            logger.error(
              s"Error getting metric ${metric.metricName()} for ${account} at ${offset} and ${
                  category.namespace
                } ${definition.name} in region ${region}",
              ex
            )
            registry
              .counter(errorStats.withTags("exception", ex.getClass.getSimpleName))
              .increment()
            promise.failure(ex)
        }
      }
    }
  }

  private def timeToRun(period: Int, offset: Int, account: String, region: Region): Long = {
    // see if we've past the next run time.
    var nextRun = 0L
    try {
      val previousRun = processor.lastSuccessfulPoll(runKey(offset, account, region))
      nextRun = runAfter(offset, period)
      if (previousRun >= nextRun) {
        logger.info(
          s"Skipping CloudWatch polling for ${offset}s as we're within the polling interval. Previous ${previousRun}. Next ${nextRun}"
        )
        return -1
      } else {
        logger.info(
          s"Polling for offset ${offset}s. Previous run was at ${previousRun}. Next run at ${nextRun}"
        )
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Unexpected exception checking for the last poll timestamp on ${offset}s", ex)
        return -1
    }
    nextRun
  }

}

object CloudWatchPoller {

  private def runAfter(offset: Int, period: Int): Long = {
    val now = System.currentTimeMillis()
    val midnight = normalize(now, period)
    val start = midnight + (offset * 1000)
    if (start < now) start + (period * 1000) else start
  }

  private[cloudwatch] def runKey(offset: Int, account: String, region: Region): String = {
    s"${account}_${region.toString}_${offset.toString}"
  }

}
