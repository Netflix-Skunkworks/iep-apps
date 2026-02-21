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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDimensions
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
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest
import software.amazon.awssdk.services.cloudwatch.model.Metric
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.jdk.StreamConverters.*
import scala.util.Failure
import scala.util.Success

/**
 * Poller for CloudWatch metrics.
 *
 * - `pollListMetrics` runs ListMetrics on a slower cadence and populates a cache.
 * - `poll` uses the cached metric list to fetch datapoints (GetMetricData / GetMetricStatistics).
 *
 * High-resolution metrics (period < 60s):
 * - Use hrmFrequency as polling interval.
 * - Use hrmListFrequency as discovery interval.
 * - Deduplicate datapoints via highResTimeCache.
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
  // The metric didn't have an update since the last post
  private val hrmStale = registry.createId("atlas.cloudwatch.poller.hrm.stale")
  // We backfilled after getting an older value.
  private val hrmBackfill = registry.createId("atlas.cloudwatch.poller.hrm.backfill")
  private val hrmDuplicate = registry.createId("atlas.cloudwatch.poller.hrm.duplicate")
  // offset from wallclock in ms
  private val hrmWallOffset = registry.createId("atlas.cloudwatch.poller.hrm.wallOffset")
  private val polledPublishPath = registry.createId("atlas.cloudwatch.poller.publishPath")
  private val dpsPolled = registry.counter("atlas.cloudwatch.poller.dps.polled")

  // Global polling frequencies
  private val frequency = config.getDuration("atlas.cloudwatch.poller.frequency").getSeconds
  private val hrmFrequency = config.getDuration("atlas.cloudwatch.poller.hrmFrequency").getSeconds

  private val hrmListFrequency =
    config.getDuration("atlas.cloudwatch.poller.hrmListFrequency").getSeconds

  private val useHrmMetricsCache =
    if (config.hasPath("atlas.cloudwatch.poller.useHrmMetricsCache"))
      config.getBoolean("atlas.cloudwatch.poller.useHrmMetricsCache")
    else
      false

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
          s"Setting offset of ${offset}s for ns ${category.namespace} period ${category.period} for ${category.metrics.size} definition"
        )
        category.metrics.foreach(md => {
          logger.info(
            s"${category.namespace} :: step: ${category.period}s, name: ${md.name}, tags: ${md.tags}"
          )
        })
      }
    logger.info(s"Loaded ${map.size} polling offsets")
    map
  }

  private[cloudwatch] val fastPollingAccounts =
    config.getStringList("atlas.cloudwatch.account.polling.fastPolling")

  private[cloudwatch] val fastBatchPollingAccounts =
    config.getStringList("atlas.cloudwatch.account.polling.fastBatchPolling")

  private[cloudwatch] val awsRequestLimit =
    config.getInt("atlas.cloudwatch.account.polling.requestLimit")

  private[cloudwatch] val highResTimeCache = new ConcurrentHashMap[Long, RecentTimestamps]()

  private[cloudwatch] val hrmLookback = config.getInt("atlas.cloudwatch.poller.hrmLookback")

  // Cache for ListMetrics results: key = account|region|namespace|dims|metricName
  private[cloudwatch] val metricsCache =
    new ConcurrentHashMap[String, List[Metric]]()

  // Track previous cache sizes to log diffs per key
  private[cloudwatch] val metricsCacheSizes =
    new ConcurrentHashMap[String, Int]()

  private def cacheKey(
    account: String,
    region: Region,
    category: MetricCategory,
    definition: MetricDefinition
  ): String = {
    val dimKey = category.dimensions.mkString(",") // configured dimension names
    s"$account|${region.toString}|${category.namespace}|$dimKey|${definition.name}"
  }

  {
    // Scheduling Pollers Based on Offset
    offsetMap.foreach {
      case (offset, categories) =>
        logger.info(
          s"Scheduling poller for offset ${offset}s for total ${categories.size} categories"
        )

        val isHrm = categories.exists(_.period < 60)
        val scheduleFrequency =
          if (isHrm) hrmFrequency else frequency

        if (isHrm) {
          logger.info(
            s"Polling for ${hrmLookback} high res data points in the past. " +
              s"HRM metrics cache enabled = $useHrmMetricsCache"
          )
          if (useHrmMetricsCache) {
            system.scheduler.scheduleAtFixedRate(
              FiniteDuration(hrmListFrequency, TimeUnit.SECONDS),
              FiniteDuration(hrmListFrequency, TimeUnit.SECONDS)
            )(() => pollListMetrics(offset, categories))(system.dispatcher)
          }
        }

        // Data polling loop
        system.scheduler.scheduleAtFixedRate(
          FiniteDuration(scheduleFrequency, TimeUnit.SECONDS),
          FiniteDuration(scheduleFrequency, TimeUnit.SECONDS)
        )(() => poll(offset, categories))(system.dispatcher)
    }
  }

  /**
   * Discovery loop: periodically run ListMetrics for HRM categories and populate metricsCache.
   */
  private[cloudwatch] def pollListMetrics(
    offset: Int,
    categories: List[MetricCategory]
  ): Unit = {
    if (!leaderStatus.hasLeadership) {
      logger.info(s"Not the leader for ${offset}s, skipping CloudWatch ListMetrics polling.")
      return
    }

    if (!useHrmMetricsCache) {
      logger.info("HRM metrics cache disabled via feature flag; skipping pollListMetrics.")
      return
    }

    // Only HRM categories
    val hrmCategories = categories.filter(_.period < 60)
    if (hrmCategories.isEmpty) return

    try {
      accountSupplier.accounts.foreach {
        case (account, regions) =>
          // Only cache for fast-batch accounts
          val isFastBatch = fastBatchPollingAccounts.contains(account)
          if (!isFastBatch) {
            logger.debug(
              s"Skipping pollListMetrics for account=$account " +
                "because it is not in fastBatchPollingAccounts."
            )
          } else {
            regions.foreach {
              case (region, namespaces) =>
                if (namespaces.nonEmpty) {
                  val filtered = hrmCategories.filter(c => namespaces.contains(c.namespace))
                  if (filtered.nonEmpty) {
                    val client = createClient(account, region)

                    filtered.foreach { category =>
                      category.toListRequests.foreach {
                        case (definition, request) =>
                          val key = cacheKey(account, region, category, definition)
                          try {
                            val paginator = client.listMetricsPaginator(request)
                            val allMetrics = paginator.metrics().stream().toScala(List)
                            val kept = allMetrics.filter { m =>
                              category.dimensionsMatch(m.dimensions().asScala.toList) &&
                              !category.filter.map(_.matches(toTagMap(m))).getOrElse(false)
                            }

                            val newSize = kept.size
                            val oldSize = Option(metricsCacheSizes.get(key)).getOrElse(0)

                            // Update the cache content
                            metricsCache.put(key, kept)
                            // Track size for next time
                            metricsCacheSizes.put(key, newSize)

                            logger.info(
                              s"Updated metrics cache for key=$key " +
                                s"(account=$account, region=$region, ns=${category.namespace}, metric=${definition.name}) " +
                                s"size change: $oldSize -> $newSize"
                            )
                          } catch {
                            case ex: Exception =>
                              logger.error(
                                s"Error listing metrics (discovery) for $account ${
                                    category.namespace
                                  } ${definition.name} in region $region",
                                ex
                              )
                              registry
                                .counter(
                                  errorListing.withTags("exception", ex.getClass.getSimpleName)
                                )
                                .increment()
                          }
                      }
                    }
                  }
                }
            }
          }
      }
    } catch {
      case ex: Exception =>
        logger.error("Unexpected exception in pollListMetrics.", ex)
        registry
          .counter(errorSetup.withTags("exception", ex.getClass.getSimpleName))
          .increment()
    }
  }

  /**
   * Data polling loop: use cached metrics if available, otherwise fall back to ListMetrics once.
   */
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
    val isHrm = period < 60
    val interval =
      if (isHrm) hrmFrequency.toInt else period

    if (
      offset <= 60 &&
      !fastPollingAccounts.contains(account) &&
      !fastBatchPollingAccounts.contains(account)
    ) false
    else timeToRun(interval, offset, account, region) > 0
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

  private def timeToRun(
    intervalSeconds: Int,
    offset: Int,
    account: String,
    region: Region
  ): Long = {
    var nextRun = 0L
    try {
      val previousRun = processor.lastSuccessfulPoll(runKey(offset, account, region))
      nextRun = runAfter(offset, intervalSeconds)
      if (previousRun >= nextRun) {
        logger.info(
          s"Skipping CloudWatch polling for ${offset}s as we're within the polling interval. Previous $previousRun. Next $nextRun"
        )
        -1
      } else {
        logger.info(
          s"Polling for offset ${offset}s. Previous run was at $previousRun. Next run at $nextRun"
        )
        nextRun
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Unexpected exception checking for the last poll timestamp on ${offset}s", ex)
        -1
    }
  }

  /**
   * Per-(category, account, region, offset) worker.
   */
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

    /**
     * Returns true if this (series, timestamp) has not been seen before and records it.
     * Uses a bounded RecentTimestamps per series: size limited to 2 * hrmLookback.
     */
    private def isNewHighResDatapoint(xxHash: Long, ts: Long): Boolean = {
      val maxSize = math.max(2 * hrmLookback, 1)
      val recent = highResTimeCache.computeIfAbsent(
        xxHash,
        _ => new RecentTimestamps(maxSize)
      )
      recent.addIfNew(ts)
    }

    private[cloudwatch] def execute: Future[Done] = {
      val futures = category.toListRequests.map { tuple =>
        val (definition, request) = tuple
        val promise = Promise[Done]()
        val key = cacheKey(account, region, category, definition)
        val cached = Option(metricsCache.get(key))

        if (useHrmMetricsCache && fastBatchPollingAccounts.contains(account)) {
          cached match {
            case Some(metrics) if metrics.nonEmpty =>
              logger.info(
                s"[HRM] Using cached metrics for key=$key, size=${metrics.size} " +
                  s"(ns=${category.namespace}, metric=${definition.name}, account=$account, region=$region)"
              )
              executionContext.execute(
                UseCachedMetrics(definition, metrics, promise)
              )

            case _ =>
              logger.info(
                s"[HRM] No cached metrics or empty cache for key=$key, using ListMetrics " +
                  s"(ns=${category.namespace}, metric=${definition.name}, account=$account, region=$region)"
              )
              metricsCache.remove(key)
              executionContext.execute(ListMetrics(request, definition, promise))
          }
        } else {
          // Cache disabled or non fast-batch account → always use ListMetrics directly
          executionContext.execute(ListMetrics(request, definition, promise))
        }

        promise.future
      }

      Future.reduceLeft(futures)((_, _) => Done).andThen {
        case Success(_) =>
          logger.info(
            s"Finished polling with ${got.get()} of ${expecting.get()} for $account at $offset and ${
                category.namespace
              } in region $region in ${(System.currentTimeMillis() - nowMillis) / 1000.0} s"
          )
        case Failure(_) => // bubble up
      }
    }

    /** Shared logic for processing a concrete list of CloudWatch metrics. */
    private def processConcreteMetrics(
      metricsList: List[Metric],
      definition: MetricDefinition,
      promise: Promise[Done],
      cacheKeyOpt: Option[String]
    ): Unit = {

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
      val promises = new Array[Promise[Done]](metricsList.size)
      val batchCandidates = collection.mutable.ArrayBuffer[(Int, Metric)]()

      var idx = 0
      val throttled = Source
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
            val p = Promise[Done]()
            promises(idx) = p
            futures(idx) = p.future
            expecting.incrementAndGet()

            if (fastBatchPollingAccounts.contains(account)) {
              batchCandidates += ((idx, metric))
            } else {
              executionContext.execute(FetchMetricStats(definition, metric, p))
            }
          }
          idx += 1
        }

      throttled.onComplete {
        case Success(_) =>
          // Optionally populate cache for future polls (only for ListMetrics path)
          cacheKeyOpt.foreach { key =>
            if (useHrmMetricsCache && fastBatchPollingAccounts.contains(account)) {
              try {
                metricsCache.put(key, metricsList)
              } catch {
                case ex: Exception =>
                  logger.warn(s"Failed to update metrics cache for key $key", ex)
              }
            }
          }

          val batchFutureOpt =
            if (fastBatchPollingAccounts.contains(account) && batchCandidates.nonEmpty) {
              val metricBatches = batchCandidates.grouped(100).toList
              val batchFutures = metricBatches.map { batch =>
                val (indices, metrics) = batch.unzip
                val perMetricPromises = indices.map(promises)
                val batchPromise = Promise[Done]()
                executionContext.execute(
                  GetMetricDataBatch(
                    definition,
                    metrics.toList,
                    perMetricPromises.toList,
                    batchPromise
                  )
                )
                batchPromise.future
              }
              Some(Future.reduceLeft(batchFutures)((_, _) => Done))
            } else {
              None
            }

          val allFutures =
            batchFutureOpt match {
              case Some(batchF) =>
                for {
                  _ <- batchF
                  _ <- Future.sequence(futures.toList)
                } yield Done
              case None =>
                Future.sequence(futures.toList).map(_ => Done)
            }

          allFutures.onComplete {
            case Success(_) =>
              promise.success(Done)
            case Failure(ex) =>
              logger.error(
                s"Failed at least one polling for $account and ${category.namespace} ${definition.name} in region $region",
                ex
              )
              promise.failure(ex)
          }

        case Failure(ex) =>
          promise.failure(ex)
      }
    }

    /** Use metrics already discovered and cached by pollListMetrics. */
    private[cloudwatch] case class UseCachedMetrics(
      definition: MetricDefinition,
      metrics: List[Metric],
      promise: Promise[Done]
    ) extends Runnable {

      @Override def run(): Unit = {
        try {
          processConcreteMetrics(metrics, definition, promise, cacheKeyOpt = None)
        } catch {
          case ex: Exception =>
            logger.error(
              s"Error in UseCachedMetrics for $account and ${category.namespace} ${definition.name} in region $region",
              ex
            )
            promise.failure(ex)
        }
      }
    }

    private[cloudwatch] case class ListMetrics(
      request: ListMetricsRequest,
      definition: MetricDefinition,
      promise: Promise[Done]
    ) extends Runnable {

      @Override def run(): Unit = {
        try {
          val metricsIterable = client.listMetricsPaginator(request)
          val metricsList = metricsIterable.metrics().stream().toScala(List)

          val key = cacheKey(account, region, category, definition)

          processConcreteMetrics(
            metricsList,
            definition,
            promise,
            cacheKeyOpt = Some(key)
          )
        } catch {
          case ex: Exception =>
            logger.error(
              s"Error listing metrics for $account and ${category.namespace} ${definition.name} in region $region",
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

    private[cloudwatch] case class GetMetricDataBatch(
      definition: MetricDefinition,
      metrics: List[Metric],
      perMetricPromises: List[Promise[Done]],
      batchPromise: Promise[Done]
    ) extends Runnable {

      require(metrics.size == perMetricPromises.size, "metrics and promises size mismatch")

      private case class StatSeries(
        timestamps: collection.mutable.Buffer[Instant] = collection.mutable.Buffer.empty,
        max: collection.mutable.Buffer[java.lang.Double] = collection.mutable.Buffer.empty,
        min: collection.mutable.Buffer[java.lang.Double] = collection.mutable.Buffer.empty,
        sum: collection.mutable.Buffer[java.lang.Double] = collection.mutable.Buffer.empty,
        cnt: collection.mutable.Buffer[java.lang.Double] = collection.mutable.Buffer.empty
      )

      @Override def run(): Unit = {
        try {
          var start = now.minusSeconds(minCacheEntries * category.period)
          val (startTime, endTime) =
            if (category.period < 60) {
              var ts = now.toEpochMilli
              ts = ts - (ts % (category.period * 1000))
              registry
                .counter(hrmRequest.withTags("period", category.period.toString))
                .increment()
              start = Instant.ofEpochMilli(ts - (hrmLookback * category.period * 1000))
              (start, Instant.ofEpochMilli(ts))
            } else {
              (start, now)
            }

          val perMetricSeries = Array.fill(metrics.size)(StatSeries())
          val idToMetricAndStat = collection.mutable.Map[String, (Int, String)]()
          val queriesBuf = collection.mutable.ListBuffer[MetricDataQuery]()

          metrics.zipWithIndex.foreach {
            case (m, idx) =>
              val meta = MetricMetadata(category, definition, m.dimensions().asScala.toList)
              val baseId = s"m$idx"
              val qs = MetricMetadata.toMetricDataQueries(meta, baseId)
              qs.foreach { q =>
                queriesBuf += q
                val id = q.id()
                val suffix =
                  if (id.endsWith("_max")) "max"
                  else if (id.endsWith("_min")) "min"
                  else if (id.endsWith("_sum")) "sum"
                  else "cnt"
                idToMetricAndStat += id -> (idx, suffix)
              }
          }

          val queries = queriesBuf.toList

          val request = GetMetricDataRequest
            .builder()
            .startTime(startTime)
            .endTime(endTime)
            .metricDataQueries(queries.asJava)
            .build()

          val response = client.getMetricData(request)

          response.metricDataResults().asScala.foreach { r =>
            idToMetricAndStat.get(r.id()) match {
              case Some((metricIdx, stat)) =>
                val s = perMetricSeries(metricIdx)
                val ts = r.timestamps().asScala
                val vs = r.values().asScala

                if (ts.nonEmpty && vs.nonEmpty) {
                  stat match {
                    case "max" =>
                      s.timestamps ++= ts
                      s.max ++= vs
                    case other =>
                      if (s.timestamps.isEmpty) s.timestamps ++= ts
                      other match {
                        case "min" => s.min ++= vs
                        case "sum" => s.sum ++= vs
                        case "cnt" => s.cnt ++= vs
                      }
                  }
                }

              case None =>
                logger.warn(s"Received MetricDataResult with unknown id ${r.id()}")
            }
          }

          metrics.indices.foreach { i =>
            val m = metrics(i)
            val p = perMetricPromises(i)
            val series = perMetricSeries(i)

            // We only treat it as empty if there are no timestamps, or all stat buffers are empty.
            val hasAnyStat =
              series.max.nonEmpty || series.min.nonEmpty || series.sum.nonEmpty || series.cnt.nonEmpty

            if (series.timestamps.isEmpty || !hasAnyStat) {
              logger.info(
                s"[HRM] GetMetricDataBatch: empty series for account=$account region=$region " +
                  s"ns=${category.namespace} metricName=${m.metricName()} def=${definition.name} " +
                  s"timestamps=${series.timestamps.size} max=${series.max.size} " +
                  s"min=${series.min.size} sum=${series.sum.size} cnt=${series.cnt.size}"
              )
              debugger.debugPolled(m, IncomingMatch.DroppedEmpty, nowMillis, category)
              got.incrementAndGet()
              p.success(Done)
            } else {
              val datapoints = buildDatapointList(series, definition)
              processMetricDatapoints(definition, m, datapoints, endTime.toEpochMilli)
              got.incrementAndGet()
              p.success(Done)
            }
          }

          batchPromise.success(Done)

        } catch {
          case ex: Exception =>
            logger.error(
              s"Error getting metric data batch for $account at $offset and ${category.namespace} ${definition.name} in region $region",
              ex
            )
            registry
              .counter(errorStats.withTags("exception", ex.getClass.getSimpleName))
              .increment()
            perMetricPromises.foreach(_.tryFailure(ex))
            batchPromise.failure(ex)
        }
      }

      private def buildDatapointList(
        s: StatSeries,
        definition: MetricDefinition
      ): List[Datapoint] = {
        val out = collection.mutable.ListBuffer[Datapoint]()
        var i = 0

        val standardUnit: StandardUnit =
          Option(definition.unit)
            .filter(_.nonEmpty)
            .flatMap { u =>
              scala.util.Try(StandardUnit.fromValue(u)).toOption
            }
            .getOrElse(StandardUnit.NONE)

        while (i < s.timestamps.size) {
          val builder = Datapoint
            .builder()
            .timestamp(s.timestamps(i))
            .maximum(if (i < s.max.size) s.max(i) else null)
            .minimum(if (i < s.min.size) s.min(i) else null)
            .sum(if (i < s.sum.size) s.sum(i) else null)
            .sampleCount(if (i < s.cnt.size) s.cnt(i) else null)
            .unit(standardUnit)

          out += builder.build()
          i += 1
        }
        out.toList
      }
    }

    private[cloudwatch] case class FetchMetricStats(
      definition: MetricDefinition,
      metric: Metric,
      promise: Promise[Done]
    ) extends Runnable {

      @Override def run(): Unit = {
        try {
          var start = now.minusSeconds(minCacheEntries * category.period)
          val request: GetMetricStatisticsRequest =
            if (category.period < 60) {
              var ts = now.toEpochMilli
              ts = ts - (ts % (category.period * 1000))
              registry
                .counter(hrmRequest.withTags("period", category.period.toString))
                .increment()
              start = Instant.ofEpochMilli(ts - (hrmLookback * category.period * 1000))
              MetricMetadata(category, definition, metric.dimensions().asScala.toList)
                .toGetRequest(start, Instant.ofEpochMilli(ts))
            } else {
              MetricMetadata(category, definition, metric.dimensions().asScala.toList)
                .toGetRequest(start, now)
            }

          val response = client.getMetricStatistics(request)

          if (response.datapoints().isEmpty) {
            debugger.debugPolled(metric, IncomingMatch.DroppedEmpty, nowMillis, category)
          } else {
            val endMs = request.endTime().toEpochMilli
            processMetricDatapoints(definition, metric, response.datapoints().asScala, endMs)
          }
          got.incrementAndGet()
          promise.success(Done)
        } catch {
          case ex: Exception =>
            logger.error(
              s"Error getting metric ${metric.metricName()} for $account at $offset and ${
                  category.namespace
                } ${definition.name} in region $region",
              ex
            )
            registry
              .counter(errorStats.withTags("exception", ex.getClass.getSimpleName))
              .increment()
            promise.failure(ex)
        }
      }
    }

    /** High-res: dedupe via highResTimeCache; low-res: update cache. */
    private[cloudwatch] def processMetricDatapoints(
      definition: MetricDefinition,
      metric: Metric,
      datapoints: Iterable[Datapoint],
      endTimeMillis: Long
    ): Unit = {
      val dims = metric.dimensions().asScala.toBuffer
      dims += Dimension.builder().name("nf.account").value(account).build()
      dims += Dimension.builder().name("nf.region").value(region.toString).build()
      val dimensions = dims.toList

      val dpList = datapoints.toList

      if (dpList.isEmpty) {
        debugger.debugPolled(metric, IncomingMatch.DroppedEmpty, endTimeMillis, category)
        return
      }

      if (category.period < 60) {
        // high-res path publish to registry
        var foundValidData = false
        dpList.foreach { dp =>
          val firehoseMetric =
            FirehoseMetric("", metric.namespace(), metric.metricName(), dimensions, dp)

          val ts = dp.timestamp().toEpochMilli
          val hash = firehoseMetric.xxHash

          // Deduplicate per (series, timestamp), keeping only a bounded recent history
          if (isNewHighResDatapoint(hash, ts)) {
            val metaData = MetricMetadata(category, definition, toAWSDimensions(firehoseMetric))
            registry.counter(polledPublishPath.withTag("path", "registry")).increment()
            processor.sendToRegistry(metaData, firehoseMetric, nowMillis)

            foundValidData = true
            debugger.debugPolled(metric, IncomingMatch.Accepted, endTimeMillis, category)

            if (endTimeMillis - ts > category.period * 1000L) {
              registry
                .counter(
                  hrmBackfill.withTags(
                    "aws.namespace",
                    category.namespace,
                    "aws.metric",
                    definition.name
                  )
                )
                .increment()
            }

            registry
              .distributionSummary(
                hrmWallOffset.withTags(
                  "aws.namespace",
                  category.namespace,
                  "aws.metric",
                  definition.name
                )
              )
              .record(System.currentTimeMillis() - ts)
          } else {
            registry
              .counter(
                hrmDuplicate.withTags(
                  "aws.namespace",
                  category.namespace,
                  "aws.metric",
                  definition.name
                )
              )
              .increment()
          }
        }

        if (!foundValidData) {
          registry
            .counter(
              hrmStale.withTags(
                "aws.namespace",
                category.namespace,
                "aws.metric",
                definition.name
              )
            )
            .increment()
          debugger.debugPolled(metric, IncomingMatch.Stale, endTimeMillis, category, dpList.asJava)
        }

      } else {
        // regular path: update cache
        debugger.debugPolled(
          metric,
          IncomingMatch.Accepted,
          endTimeMillis,
          category,
          dpList.asJava
        )

        dpList.foreach { dp =>
          val firehoseMetric =
            FirehoseMetric("", metric.namespace(), metric.metricName(), dimensions, dp)
          registry.counter(polledPublishPath.withTag("path", "cache")).increment()
          processor.updateCache(firehoseMetric, category, nowMillis).onComplete {
            case Success(_)  => logger.debug("Cache update success")
            case Failure(ex) => logger.error(s"Cache update failed: ${ex.getMessage}")
          }
        }
      }
    }
  }

  // Helper structure to track recent timestamps per series with bounded size.
  class RecentTimestamps(maxSize: Int) {

    private val deque = new ConcurrentLinkedDeque[Long]()

    private val set = java.util.Collections.newSetFromMap(
      new java.util.concurrent.ConcurrentHashMap[Long, java.lang.Boolean]()
    )

    /** Returns true if `ts` was not seen before and records it. Oldest entries are evicted if needed. */
    def addIfNew(ts: Long): Boolean = this.synchronized {
      if (set.contains(ts)) {
        false
      } else {
        // Add new timestamp
        deque.addLast(ts)
        set.add(ts)

        // Evict oldest if we exceed the bound
        while (deque.size() > maxSize) {
          val oldest = deque.pollFirst()
          set.remove(oldest)
        }
        true
      }
    }
  }
}

object CloudWatchPoller {

  private def runAfter(offset: Int, intervalSeconds: Int): Long = {
    val now = System.currentTimeMillis()
    val midnight = normalize(now, intervalSeconds)
    val start = midnight + (offset * 1000)
    if (start < now) start + (intervalSeconds * 1000L) else start
  }

  private[cloudwatch] def runKey(offset: Int, account: String, region: Region): String = {
    s"${account}_${region.toString}_${offset.toString}"
  }
}
