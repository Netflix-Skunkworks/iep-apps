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
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException
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
  // Cached ListMetrics results that were not refreshed within the staleness threshold
  private val staleCache = registry.createId("atlas.cloudwatch.poller.staleCache")

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

  // Epoch millis of the last successful discovery refresh per key. Used to detect a key
  // that is stuck serving a stale list (e.g. because discovery keeps failing/throttling
  // for it) so new dimension combinations don't silently go missing indefinitely.
  private[cloudwatch] val metricsCacheLastRefresh =
    new ConcurrentHashMap[String, Long]()

  // A cached metrics list older than this is no longer trusted blindly; the data-poll
  // path will force a live ListMetrics fetch instead. A few missed discovery cycles are
  // tolerated before treating the cache as stale.
  private[cloudwatch] val cacheStalenessMs = hrmListFrequency * 1000 * 3

  // Discovery cadence is throttled per account (CloudWatch's ListMetrics limit is per
  // account/region), and this tracks the current effective per-account rate. An account
  // that's actively being rate limited gets its own rate turned down for the next cycle so
  // it backs off instead of hammering a limit it's already exceeding; other accounts run
  // on independent streams and are unaffected.
  private[cloudwatch] val accountDiscoveryLimit = new ConcurrentHashMap[String, Int]()

  // Guards against a scheduled discovery tick starting a second, overlapping run for an
  // account whose previous discovery pass (possibly slowed by backoff) hasn't finished yet.
  private[cloudwatch] val discoveryInFlight = ConcurrentHashMap.newKeySet[String]()

  private def isThrottlingException(ex: Throwable): Boolean = ex match {
    case e: CloudWatchException =>
      val code = Option(e.awsErrorDetails()).map(_.errorCode()).getOrElse("")
      code.equalsIgnoreCase("Throttling") ||
      Option(e.getMessage).exists(_.contains("Rate exceeded"))
    case _ => false
  }

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
      // Build one discovery worklist per account and throttle+run each account's stream
      // independently and concurrently. CloudWatch's ListMetrics rate limit is enforced
      // per account/region, so a single shared throttled stream across all accounts is
      // the wrong isolation boundary: runForeach processes items sequentially, so one
      // account with many dimension combinations (or one that's actively being
      // rate-limited and retrying with backoff) sits in front of the queue and starves
      // discovery for every other, healthy account behind it. Per-account streams keep a
      // throttled/misbehaving account from delaying anyone else's cache refresh.
      //
      // Priority order follows the account order declared in
      // atlas.cloudwatch.account.polling.fastBatchPolling, so the accounts operators care
      // about most get their discovery streams kicked off first.
      val priorityOrder = fastBatchPollingAccounts.asScala.toList.zipWithIndex.toMap
      val perAccountWork = accountSupplier.accounts.toList
        .filter { case (account, _) => priorityOrder.contains(account) }
        .sortBy { case (account, _) => priorityOrder(account) }
        .map {
          case (account, regions) =>
            val items = for {
              (region, namespaces) <- regions.toList
              if namespaces.nonEmpty
              category <- hrmCategories
              if namespaces.contains(category.namespace)
              (definition, request) <- category.toListRequests
            } yield (region, category, definition, request)
            account -> items
        }

      perAccountWork.foreach {
        case (account, items) =>
          if (!discoveryInFlight.add(account)) {
            logger.warn(
              s"[HRM] Discovery still in flight for account=$account from a prior cycle; " +
                "skipping this tick rather than piling up overlapping runs."
            )
          } else {
            val effectiveLimit =
              Option(accountDiscoveryLimit.get(account)).getOrElse(awsRequestLimit)
            val sawThrottling = new java.util.concurrent.atomic.AtomicBoolean(false)

            try {
              val discovery = Source
                .fromIterator(() => items.iterator)
                .throttle(effectiveLimit, 1.second)
                .runForeach {
                  case (region, category, definition, request) =>
                    val key = cacheKey(account, region, category, definition)
                    try {
                      val client = createClient(account, region)
                      val paginator = client.listMetricsPaginator(request)
                      val allMetrics = paginator.metrics().stream().toScala(List)
                      logger.info(
                        s"[HRM] ListMetrics response for key=$key (account=$account, " +
                          s"region=$region, ns=${category.namespace}, metric=${definition.name}): " +
                          s"${allMetrics.size} dimension combinations returned by AWS"
                      )
                      val kept = allMetrics.filter { m =>
                        category.dimensionsMatch(m.dimensions().asScala.toList) &&
                        !category.filter.map(_.matches(toTagMap(m))).getOrElse(false)
                      }

                      val isFirstCache = !metricsCache.containsKey(key)
                      val oldList = Option(metricsCache.get(key)).getOrElse(List.empty[Metric])
                      val newSize = kept.size
                      val oldSize = oldList.size

                      // Diff against what was cached before this refresh so newly appeared
                      // (or disappeared) dimension combinations are visible individually,
                      // not just as a net size change that can mask churn (e.g. one new
                      // combo appearing while another drops in the same cycle).
                      val added = kept.diff(oldList)
                      val removed = oldList.diff(kept)

                      // Update the cache content
                      metricsCache.put(key, kept)
                      // Track size and last-refresh time for next time
                      metricsCacheSizes.put(key, newSize)
                      metricsCacheLastRefresh.put(key, System.currentTimeMillis())

                      added.foreach { m =>
                        logger.info(
                          s"[HRM] New dimension combination cached for key=$key: " +
                            s"${toTagMap(m)} " +
                            s"(account=$account, region=$region, ns=${category.namespace}, metric=${definition.name})"
                        )
                      }
                      removed.foreach { m =>
                        logger.info(
                          s"[HRM] Dimension combination no longer active for key=$key: " +
                            s"${toTagMap(m)} " +
                            s"(account=$account, region=$region, ns=${category.namespace}, metric=${definition.name})"
                        )
                      }

                      if (isFirstCache) {
                        logger.info(
                          s"[HRM] Initial metrics cache populated for key=$key " +
                            s"(account=$account, region=$region, ns=${category.namespace}, metric=${definition.name}) " +
                            s"size: $newSize, content: ${kept.map(toTagMap)}"
                        )
                      } else if (oldSize != newSize) {
                        logger.info(
                          s"Updated metrics cache for key=$key " +
                            s"(account=$account, region=$region, ns=${category.namespace}, metric=${definition.name}) " +
                            s"size change: $oldSize -> $newSize, content: ${kept.map(toTagMap)}"
                        )
                      } else {
                        logger.info(
                          s"Refreshed metrics cache for key=$key " +
                            s"(account=$account, region=$region, ns=${category.namespace}, metric=${definition.name}) " +
                            s"size unchanged: $newSize"
                        )
                      }
                    } catch {
                      case ex: Exception =>
                        if (isThrottlingException(ex)) sawThrottling.set(true)
                        logger.error(
                          s"Error listing metrics (discovery) for $account ${category.namespace} ${definition.name} in region $region",
                          ex
                        )
                        registry
                          .counter(
                            errorListing.withTags("exception", ex.getClass.getSimpleName)
                          )
                          .increment()
                    }
                }

              // Deliberately not awaited: accounts run fully independently of each other, so
              // a slow/backed-off account never blocks discovery for the rest. The in-flight
              // guard above prevents the next scheduled tick from starting a second run for
              // this account before this one finishes, and each account's own effective rate
              // is adjusted for next cycle based on whether it hit throttling this time.
              discovery.onComplete { result =>
                if (sawThrottling.get()) {
                  val backedOff = math.max(1, effectiveLimit / 2)
                  accountDiscoveryLimit.put(account, backedOff)
                  logger.warn(
                    s"[HRM] Discovery for account=$account hit CloudWatch throttling; " +
                      s"reducing its discovery rate to $backedOff/sec for the next cycle."
                  )
                } else {
                  val recovered = math.min(awsRequestLimit, effectiveLimit + 1)
                  accountDiscoveryLimit.put(account, recovered)
                }
                result.failed.foreach { ex =>
                  logger.error(s"Discovery stream failed for account=$account", ex)
                }
                discoveryInFlight.remove(account)
              }(system.dispatcher)
            } catch {
              case ex: Exception =>
                // Guards against synchronous failures during stream setup/materialization
                // (e.g. "Trying to materialize stream after materializer has been shutdown").
                // Without this, such an exception would both skip every account still left
                // in this tick's iteration and leave this account stuck in discoveryInFlight
                // forever, since .onComplete would never be attached to release it.
                logger.error(s"Failed to start discovery stream for account=$account", ex)
                registry
                  .counter(errorSetup.withTags("exception", ex.getClass.getSimpleName))
                  .increment()
                discoveryInFlight.remove(account)
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
          val lastRefresh = Option(metricsCacheLastRefresh.get(key)).getOrElse(0L)
          val cacheAgeMs = System.currentTimeMillis() - lastRefresh
          cached match {
            case Some(metrics) if metrics.nonEmpty && cacheAgeMs <= cacheStalenessMs =>
              logger.debug(
                s"[HRM] Using cached metrics for key=$key, size=${metrics.size} " +
                  s"(ns=${category.namespace}, metric=${definition.name}, account=$account, region=$region)"
              )
              executionContext.execute(
                UseCachedMetrics(definition, metrics, promise)
              )

            case Some(metrics) if metrics.nonEmpty =>
              // Discovery hasn't refreshed this key recently enough to trust it blindly;
              // a new dimension combination could have appeared and gone missing. Force a
              // live ListMetrics fetch instead of continuing to serve the stale list.
              logger.warn(
                s"[HRM] Cache for key=$key is stale (age=${cacheAgeMs}ms, " +
                  s"threshold=${cacheStalenessMs}ms), forcing live ListMetrics " +
                  s"(ns=${category.namespace}, metric=${definition.name}, account=$account, region=$region)"
              )
              registry
                .counter(staleCache.withTags("aws.namespace", category.namespace))
                .increment()
              executionContext.execute(ListMetrics(request, definition, promise))

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
      cacheKeyOpt: Option[(String, Long)]
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
          cacheKeyOpt.foreach {
            case (key, fetchStartedAt) =>
              if (useHrmMetricsCache && fastBatchPollingAccounts.contains(account)) {
                try {
                  val lastRefresh = Option(metricsCacheLastRefresh.get(key)).getOrElse(0L)
                  if (lastRefresh >= fetchStartedAt) {
                    // Some other refresh (discovery cycle or another live fetch) already
                    // wrote a view of this key that is as new as or newer than the one we
                    // just fetched here. Writing ours now would regress the cache to an
                    // older snapshot while also resetting its last-refresh clock, making the
                    // stale regression look freshly refreshed. Skip it.
                    logger.info(
                      s"[HRM] Skipping cache write for key=$key (account=$account, " +
                        s"region=$region, ns=${category.namespace}, metric=${definition.name}, " +
                        s"via=live-fetch): existing cache entry (refreshed at $lastRefresh) is " +
                        s"already as fresh as this fetch (started at $fetchStartedAt)."
                    )
                  } else {
                    // Cache the same filtered view the discovery loop caches, so this
                    // fallback path doesn't pollute the cache with dimension combinations
                    // that dimensionsMatch/filter would otherwise have excluded.
                    val keptForCache = metricsList.filter { m =>
                      category.dimensionsMatch(m.dimensions().asScala.toList) &&
                      !category.filter.map(_.matches(toTagMap(m))).getOrElse(false)
                    }

                    val isFirstCache = !metricsCache.containsKey(key)
                    val oldList = Option(metricsCache.get(key)).getOrElse(List.empty[Metric])
                    // Merge rather than overwrite: this fetch's own request/response cycle
                    // can lag behind a discovery cycle that ran concurrently, so a plain
                    // replace can drop dimension combinations discovery already added. This
                    // path only ever grows the cache; discovery remains the sole source that
                    // removes entries once they're no longer reported by ListMetrics.
                    val merged =
                      if (oldList.nonEmpty) (oldList ++ keptForCache).distinct
                      else keptForCache
                    val added = merged.diff(oldList)

                    metricsCache.put(key, merged)
                    metricsCacheLastRefresh.put(key, System.currentTimeMillis())

                    added.foreach { m =>
                      logger.info(
                        s"[HRM] New dimension combination cached for key=$key: " +
                          s"${toTagMap(m)} (account=$account, region=$region, " +
                          s"ns=${category.namespace}, metric=${definition.name}, via=live-fetch)"
                      )
                    }

                    if (isFirstCache) {
                      logger.info(
                        s"[HRM] Initial metrics cache populated for key=$key (account=$account, " +
                          s"region=$region, ns=${category.namespace}, metric=${definition.name}, " +
                          s"via=live-fetch) size: ${merged.size}, content: ${merged.map(toTagMap)}"
                      )
                    } else if (oldList.size != merged.size) {
                      logger.info(
                        s"Updated metrics cache for key=$key (account=$account, region=$region, " +
                          s"ns=${category.namespace}, metric=${definition.name}, via=live-fetch) " +
                          s"size change: ${oldList.size} -> ${merged.size}, " +
                          s"content: ${merged.map(toTagMap)}"
                      )
                    } else {
                      logger.info(
                        s"Refreshed metrics cache for key=$key (account=$account, region=$region, " +
                          s"ns=${category.namespace}, metric=${definition.name}, via=live-fetch) " +
                          s"size unchanged: ${merged.size}"
                      )
                    }
                  }
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
          // Captured before issuing the AWS call so a slow/retried fetch can be detected
          // as stale relative to any discovery-cycle refresh that lands while it's in flight.
          val fetchStartedAt = System.currentTimeMillis()
          val metricsIterable = client.listMetricsPaginator(request)
          val metricsList = metricsIterable.metrics().stream().toScala(List)

          val key = cacheKey(account, region, category, definition)
          logger.info(
            s"[HRM] ListMetrics response for key=$key (account=$account, region=$region, " +
              s"ns=${category.namespace}, metric=${definition.name}, via=live-fetch): " +
              s"${metricsList.size} dimension combinations returned by AWS"
          )

          processConcreteMetrics(
            metricsList,
            definition,
            promise,
            cacheKeyOpt = Some((key, fetchStartedAt))
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
              logger.debug(
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
