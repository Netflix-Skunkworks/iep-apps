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
package com.netflix.atlas.cloudwatch

import akka.Done
import akka.actor.ActorSystem
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toTagMap
import com.netflix.atlas.cloudwatch.CloudWatchPoller.runAfter
import com.netflix.atlas.util.ExecutorFactory
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest
import software.amazon.awssdk.services.cloudwatch.model.Metric

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
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
  * TODO - Akka/Pekka streamify it. It should be simple but I had a hard time parallelizing
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
  executorFactory: ExecutorFactory,
  debugger: CloudWatchDebugger
)(implicit val system: ActorSystem)
    extends StrictLogging {

  private val pollTime = registry.timer("atlas.cloudwatch.poller.pollTime")
  private val errorSetup = registry.createId("atlas.cloudwatch.poller.failure", "call", "setup")
  private val errorListing = registry.createId("atlas.cloudwatch.poller.failure", "call", "list")
  private val errorStats = registry.createId("atlas.cloudwatch.poller.failure", "call", "metric")
  private val emptyListing = registry.createId("atlas.cloudwatch.poller.emptyList")

  private val dpsDroppedTags =
    registry.counter("atlas.cloudwatch.poller.dps.dropped", "reason", "tags")

  private val dpsDroppedFilter =
    registry.counter("atlas.cloudwatch.poller.dps.dropped", "reason", "filter")
  private val dpsExpected = registry.counter("atlas.cloudwatch.poller.dps.expected")
  private val dpsPolled = registry.counter("atlas.cloudwatch.poller.dps.polled")

  private val numThreads = config.getInt("atlas.cloudwatch.poller.num-threads")
  private val frequency = config.getDuration("atlas.cloudwatch.poller.frequency").getSeconds

  private val periodFilter =
    config.getDuration("atlas.cloudwatch.poller.period-filter").getSeconds.toInt

  private[cloudwatch] val (offsetMap, flags) = {
    var map = Map.empty[Int, List[MetricCategory]]
    var flagMap = Map.empty[Int, AtomicBoolean]
    rules
      .getCategories(config)
      .filter(c => c.pollOffset.isDefined && c.period == periodFilter)
      .foreach { category =>
        val offset = category.pollOffset.get.getSeconds.toInt
        val categories = map.getOrElse(offset, List.empty)
        map += offset -> (categories :+ category)

        val flag = flagMap.getOrElse(offset, new AtomicBoolean(false))
        flagMap += offset -> flag
        logger.info(s"Setting offset of ${offset}s for categories ${category.namespace}")
      }
    logger.info(s"Loaded ${map.size} polling offsets")
    (map, flagMap)
  }

  {
    offsetMap.foreachEntry { (offset, categories) =>
      logger.info(s"Scheduling poller for offset ${offset}s")
      system.scheduler.scheduleAtFixedRate(
        FiniteDuration.apply(frequency, TimeUnit.SECONDS),
        FiniteDuration.apply(frequency, TimeUnit.SECONDS)
      )(() => poll(offset, categories, flags.get(offset).get))(system.dispatcher)
    }
  }

  private[cloudwatch] def poll(
    offset: Int,
    categories: List[MetricCategory],
    flag: AtomicBoolean,
    fullRunUt: Option[Promise[List[CloudWatchPoller#Poller]]] = None,
    accountsUt: Option[Promise[Done]] = None
  ): Unit = {
    if (!leaderStatus.hasLeadership) {
      logger.info("Not the leader, skipping CloudWatch polling.")
      fullRunUt.map(_.success(List.empty))
      return
    }

    // see if we've past the next run time.
    var nextRun = 0L
    try {
      val previousRun = processor.lastSuccessfulPoll
      nextRun = runAfter(offset, periodFilter)
      if (previousRun >= nextRun) {
        logger.info(
          s"Skipping CloudWatch polling as we're within the polling interval. Previous ${previousRun}. Next ${nextRun}"
        )
        fullRunUt.map(_.success(List.empty))
        return
      } else {
        logger.info(
          s"Polling for offset ${offset}s. Previous run was at ${previousRun}. Next run at ${nextRun}"
        )
      }
    } catch {
      case ex: Exception =>
        logger.error("Unexpected exception checking for the last poll timestamp", ex)
        return
    }

    if (!flag.compareAndSet(false, true)) {
      logger.warn(s"Skipping polling for period ${offset}s as it was already running.")
      fullRunUt.map(_.success(List.empty))
      return
    }

    val threadPool = executorFactory.createFixedPool(numThreads)
    val start = System.currentTimeMillis()
    val futures = List.newBuilder[Future[Done]]
    val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val runners = List.newBuilder[Poller]

    try {
      accountSupplier.accounts.onComplete {
        case Success(accounts) =>
          try {
            accounts.foreachEntry { (account, regions) =>
              regions.foreach { region =>
                val client = clientFactory.getInstance(
                  account + "." + region.toString,
                  classOf[CloudWatchClient],
                  account,
                  Optional.of(region)
                )
                categories.foreach { category =>
                  val runner = Poller(now, category, threadPool, client, account, region)
                  this.synchronized {
                    runners += runner
                    futures += runner.execute
                  }
                }
              }
            }

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
                  s"Finished CloudWatch polling with ${got} of ${expecting} metrics in ${(System.currentTimeMillis() - start) / 1000.0} s"
                )
                pollTime.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
                processor.updateLastSuccessfulPoll(nextRun)
                threadPool.shutdown()
                fullRunUt.map(_.success(runners.result()))
                flag.set(false)
              case Failure(ex) =>
                logger.error(
                  "Failure at some point in polling for CloudWatch data." +
                    " Not updating the next run time so we can retry.",
                  ex
                )
                pollTime.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
                threadPool.shutdown()
                fullRunUt.map(_.failure(ex))
                flag.set(false)
            }
            accountsUt.map(_.success(Done))
          } catch {
            case ex: Exception =>
              logger.error("Unexpected exception polling for CloudWatch data.", ex)
              registry
                .counter(errorSetup.withTags("exception", ex.getClass.getSimpleName))
                .increment()
              threadPool.shutdown()
              accountsUt.map(_.failure(ex))
              fullRunUt.map(_.failure(ex))
              flag.set(false)
          }

        case Failure(ex) =>
          registry.counter(errorSetup.withTags("exception", ex.getClass.getSimpleName)).increment()
          flag.set(false)
          logger.error("Failure fetching accounts", ex)
          accountsUt.map(_.failure(ex))
          fullRunUt.map(_.failure(ex))
      }
    } catch {
      case ex: Exception =>
        registry.counter(errorSetup.withTags("exception", ex.getClass.getSimpleName)).increment()
        logger.error("Unexpected exception", ex)
        flag.set(false)
        accountsUt.map(_.failure(ex))
        fullRunUt.map(_.failure(ex))
    }
  }

  case class Poller(
    now: Instant,
    category: MetricCategory,
    threadPool: ExecutorService,
    client: CloudWatchClient,
    account: String,
    region: Region
  ) {

    private val nowMillis = now.toEpochMilli
    private[cloudwatch] val expecting = new AtomicInteger()
    private[cloudwatch] val got = new AtomicInteger()

    private[cloudwatch] def execute: Future[Done] = {
      logger.info(s"Polling for account ${account} and category ${category.namespace} in ${region}")
      val futures = category.toListRequests.map { tuple =>
        val (definition, request) = tuple
        val promise = Promise[Done]()
        threadPool.submit(ListMetrics(request, definition, promise))
        promise.future
      }

      Future.reduceLeft(futures)((_, _) => Done).andThen {
        case Success(_) =>
          logger.info(
            s"Finished polling with ${got.get()} of ${expecting.get()} for ${account} and ${
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

      @Override def run(): Unit = {
        try {
          val metrics = client.listMetricsPaginator(request)
          val metricsList = metrics.metrics().stream().toScala(List)
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
          metricsList.foreach { metric =>
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
              threadPool.submit(FetchMetricStats(definition, metric, promise))
            }
            idx += 1
          }

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
          val start = now.minusSeconds(category.periodCount * category.period)
          val request = MetricMetadata(category, definition, metric.dimensions.asScala.toList)
            .toGetRequest(start, now)
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
              processor.updateCache(m, category, nowMillis)
            }
          got.incrementAndGet()
          promise.success(Done)
        } catch {
          case ex: Exception =>
            logger.error(
              s"Error getting metric ${metric.metricName()} for ${account} and ${category.namespace} ${definition.name} in region ${region}",
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

}

object CloudWatchPoller {

  private def runAfter(offset: Int, period: Int): Long = {
    val now = System.currentTimeMillis()
    val midnight = normalize(now, period)
    val start = midnight + (offset * 1000)
    if (start < now) start + (period * 1000) else start
  }

}
