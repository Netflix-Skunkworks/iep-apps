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

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newValue
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDatapoint
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toAWSDimensions
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toTagMap
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.Metric

import java.time.Instant
import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.FiniteDuration

/**
  * Base class for a processor that evaluates incoming CloudWatch metrics against the configured rule set and passes
  * them on to a cache that will sort and publish the data at the appropriate time.
  *
  * Note that the base class schedules publishing based on the configured step interval (default of 60 seconds).
  *
  * Also note that we store only the base tags and data from Cloudwatch, not the enriched or converted values. This allows
  * us to store a bit less data in the cache. However we do validate the data twice through the rules. This also give us
  * the ability to update rules and clear out anything from the cache that is no longer relevant.
  */
abstract class CloudWatchMetricsProcessor(
  config: Config,
  registry: Registry,
  rules: CloudWatchRules,
  tagger: Tagger,
  publishRouter: PublishRouter,
  debugger: CloudWatchDebugger
)(implicit val system: ActorSystem)
    extends StrictLogging {

  private val minCacheEntries = config.getInt("atlas.cloudwatch.min-cache-entries")
  private val gracePeriod = config.getInt("atlas.cloudwatch.grace")

  /** The number of data points received by the processor from cloud watch.  */
  private[cloudwatch] val received = registry.counter("atlas.cloudwatch.datapoints.received")
  private[cloudwatch] val receivedAge = registry.createId("atlas.cloudwatch.datapoints.age")

  /** The number of data points filtered out before caching due to not having a namespace config. */
  private[cloudwatch] val filteredNS =
    registry.createId("atlas.cloudwatch.datapoints.filtered", "reason", "namespace")

  /** The number of data points filtered out before caching due to not having a metric config. */
  private[cloudwatch] val filteredMetric =
    registry.createId("atlas.cloudwatch.datapoints.filtered", "reason", "metric")

  /** The number of data points filtered out before caching due to missing or extraneous tags not definied in the category. */
  private[cloudwatch] val filteredTags =
    registry.createId("atlas.cloudwatch.datapoints.filtered", "reason", "tags")

  /** The number of data points filtered out before caching due to a query filter. */
  private[cloudwatch] val filteredQuery =
    registry.createId("atlas.cloudwatch.datapoints.filtered", "reason", "query")

  /** The number of data points written to cache. */
  private[cloudwatch] val insert = registry.createId("atlas.cloudwatch.datapoints.insert")

  /** The number of data points dropped reading from the cache due to the namespace config disappearing. */
  private[cloudwatch] val purgedNS =
    registry.counter("atlas.cloudwatch.datapoints.purged", "reason", "namespace")

  /** The number of data points dropped reading from the cache due to the metric config disappearing. */
  private[cloudwatch] val purgedMetric =
    registry.counter("atlas.cloudwatch.datapoints.purged", "reason", "metric")

  /** The number of data points dropped reading from the cache due to the tags configuration changing. */
  private[cloudwatch] val purgedTags =
    registry.counter("atlas.cloudwatch.datapoints.purged", "reason", "tags")

  /** The number of data points dropped reading from the cache due to an updated query filter. */
  private[cloudwatch] val purgedQuery =
    registry.counter("atlas.cloudwatch.datapoints.purged", "reason", "query")

  /** The number of cache entries that had empty data lists when being loaded. Usually happens due to an insert being too
    * old and existing values being expired. */
  private[cloudwatch] val publishEmpty = registry.createId("atlas.cloudwatch.publish.empty")

  /** How many values were read from the cache for publishing. */
  private[cloudwatch] val scraped = registry.createId("atlas.cloudwatch.publish.scraped")

  /** Distribution of offsets from the end of the cache entry array. Used to track how far we're looking
    * back for data. */
  private[cloudwatch] val indexOffset = registry.createId("atlas.cloudwatch.publish.indexOffset")

  /** How far the value published is from the scrape timestamp. */
  private[cloudwatch] val wallOffset = registry.createId("atlas.cloudwatch.publish.wallOffset")

  /** The number of values that were stored in the cache before a published value. */
  private[cloudwatch] val unpublished = registry.createId("atlas.cloudwatch.publish.unpublished")

  /** Values successfully published. */
  private[cloudwatch] val published = registry.createId("atlas.cloudwatch.publish.published")

  /** Values deemed to be invalid and unpublishable. */
  private[cloudwatch] val invalid = registry.createId("atlas.cloudwatch.publish.invalid")

  /** Exceptions occurring during publishing. */
  private[cloudwatch] val publishEx = registry.createId("atlas.cloudwatch.publish.exception")

  /** How many entries are updated during a scrape with published flags. */
  private[cloudwatch] val cacheUpdates = registry.createId("atlas.cloudwatch.publish.cache.updates")

  /** How often to scrape and publish. */
  private val publishPeriod = config.getDuration("atlas.cloudwatch.step").getSeconds.toInt

  /** How long to wait from the top of the publishPeriod before scraping. */
  private val publishDelay = config.getDuration("atlas.cloudwatch.publishOffset").getSeconds.toInt

  // ctor
  {
    val delay = {
      val now = System.currentTimeMillis()
      val topOfMinute = normalize(now, publishPeriod)
      ((publishPeriod * 1000) - (now - topOfMinute)) + (publishDelay * 1000)
    }
    logger.info(s"Starting publishing in ${delay / 1000.0} seconds.")
    system.scheduler.scheduleAtFixedRate(
      FiniteDuration.apply(delay, TimeUnit.MILLISECONDS),
      FiniteDuration.apply(publishPeriod, TimeUnit.SECONDS)
    )(() => {
      try {
        publish(normalize(System.currentTimeMillis(), publishPeriod))
      } catch {
        case ex: Exception =>
          logger.error("Whoops!?!?", ex)
      }
    })(system.dispatcher)
  }

  /**
    * Evaluates the data against the rules and forwards it on to the cache if valid.
    *
    * @param datapoints
    *     A non-null list of data points for processing.
    * @param receivedTimestamp
    *     The millisecond epoch timestamp when the values came in. Re-used to avoid fetching the current
    *     time for every timestamp when evaluating latencies or expirations.
    *     <b>Note:</b> Not normalized at this point.
    */
  def processDatapoints(datapoints: List[FirehoseMetric], receivedTimestamp: Long): Unit = {
    received.increment(datapoints.size)

    datapoints.foreach { dp =>
      rules.rules.get(dp.namespace) match {
        case None =>
          registry.counter(filteredNS.withTag("aws.namespace", dp.namespace)).increment()
          debugger.debugIncoming(dp, IncomingMatch.DroppedNS, receivedTimestamp)
        case Some(namespaceRules) =>
          namespaceRules.get(dp.metricName) match {
            case None =>
              registry
                .counter(
                  filteredMetric
                    .withTag("aws.namespace", dp.namespace)
                    .withTag("aws.metric", dp.metricName)
                )
                .increment()
              debugger.debugIncoming(dp, IncomingMatch.DroppedMetric, receivedTimestamp)
            case Some(categories) =>
              var matchedDimensions = false
              categories.foreach { tuple =>
                val (category, _) = tuple
                if (category.dimensionsMatch(dp.dimensions)) {
                  matchedDimensions = true
                  if (category.filter.isDefined && tuple._1.filter.get.matches(toTagMap(dp))) {
                    registry
                      .counter(filteredQuery.withTag("aws.namespace", dp.namespace))
                      .increment()
                    debugger.debugIncoming(
                      dp,
                      IncomingMatch.DroppedFilter,
                      receivedTimestamp,
                      Some(category)
                    )
                  } else {
                    // finally passed the checks.
                    updateCache(dp, category, receivedTimestamp)
                    val delay = receivedTimestamp - dp.datapoint.timestamp().toEpochMilli
                    registry
                      .distributionSummary(receivedAge.withTag("aws.namespace", dp.namespace))
                      .record(delay)
                  }
                }
              }

              if (!matchedDimensions) {
                registry
                  .counter(
                    filteredTags
                      .withTag("aws.namespace", dp.namespace)
                      .withTag("aws.metric", dp.metricName)
                  )
                  .increment()
                debugger.debugIncoming(
                  dp,
                  IncomingMatch.DroppedTag,
                  receivedTimestamp,
                  Some(categories.head._1)
                )
              }
          }

      }
    }
  }

  /**
    * Called when a data point matched a category and should be processed by the cache.
    *
    * @param datapoint
    *     The non-null datapoint to cache.
    * @param category
    *     The non-null category the data point matched.
    * @param receivedTimestamp
    *     The receive time in unix epoch milliseconds.
    */
  protected[cloudwatch] def updateCache(
    datapoint: FirehoseMetric,
    category: MetricCategory,
    receivedTimestamp: Long
  ): Unit

  /**
    * Called when a cache entry has been mutated during publishing and must be written
    * back to the cache.
    *
    * @param key
    *     The non-null key to update. It's an `Any` for now since the key could be in any format.
    * @param prev
    *     The previous entry in the cache. This is used to determine if the entry has changed during
    *     scraping.
    * @param entry
    *     The new entry to write back to the cache.
    * @param expiration
    *     The unix epoch milliseconds when the entry should be considered expired.
    */
  protected[cloudwatch] def updateCache(
    key: Any,
    prev: CloudWatchCacheEntry,
    entry: CloudWatchCacheEntry,
    expiration: Long
  ): Unit

  /**
    * Called by the scheduler to publish data accumulated in the cache. Exposed for unit testing.
    */
  protected[cloudwatch] def publish(scrapeTimestamp: Long): Future[NotUsed]

  /**
    * Removes the given entry from the cache. This is used to purge entries that are no longer valid due to a config
    * change or error.
    *
    * @param key
    *     The non-null key to delete. It's an `Any` for now since the key could be in any format.
    */
  protected[cloudwatch] def delete(key: Any): Unit

  /**
    * Returns the last successful poll time.
    * @param id
    *     The non-null unique identifier for the polling config.
    * @return
    *     The last successful poll time in unix epoch milliseconds.
    */
  protected[cloudwatch] def lastSuccessfulPoll(id: String): Long

  /**
    * Updates the last successful poll time.
    *
    * @param id
    *     The non-null unique identifier for the polling config.
    * @param timestamp
    *     The unix epoch milliseconds of the last successful poll.
    */
  protected[cloudwatch] def updateLastSuccessfulPoll(id: String, timestamp: Long): Unit

  /**
    * Inserts the given data point in the proper order of the CloudWatchCloudWatchCacheEntry **AND** expires any old data from
    * the entry. Note that this can lead to empty cache entries and those should likely be deleted.
    * Duplicates are overwritten.
    */
  private[cloudwatch] def insertDatapoint(
    data: Array[Byte],
    datapoint: FirehoseMetric,
    category: MetricCategory,
    receivedTimestamp: Long
  ): CloudWatchCacheEntry = {
    val entry = CloudWatchCacheEntry.parseFrom(data)
    var added = false
    var i = 0
    val list = new util.LinkedList[CloudWatchValue](entry.getDataList)
    while (i < list.size() && !added) {
      val dp = list.get(i)
      if (dp.getTimestamp == datapoint.datapoint.timestamp().toEpochMilli) {
        val diffValue =
          !(datapoint.datapoint.sum() == dp.getSum &&
          datapoint.datapoint.minimum() == dp.getMin &&
          datapoint.datapoint.maximum() == dp.getMax &&
          datapoint.datapoint.sampleCount() == dp.getCount)
        if (diffValue) {
          registry
            .counter(
              insert
                .withTags(
                  "aws.namespace",
                  category.namespace,
                  "aws.metric",
                  datapoint.metricName,
                  "state",
                  "updated"
                )
            )
            .increment()
          debugger.debugIncoming(
            datapoint,
            IncomingMatch.Accepted,
            receivedTimestamp,
            Some(category),
            Some(InsertState.Update),
            list
          )
          // NOTE: We are resetting the publish bit on an update for a potential repub.
          // This is a bit of a hack but it's better than dropping the data.
          list.set(i, newValue(datapoint.datapoint, receivedTimestamp))
        } else {
          registry
            .counter(
              insert
                .withTags(
                  "aws.namespace",
                  category.namespace,
                  "aws.metric",
                  datapoint.metricName,
                  "state",
                  "duplicates"
                )
            )
            .increment()

          debugger.debugIncoming(
            datapoint,
            IncomingMatch.Accepted,
            receivedTimestamp,
            Some(category),
            Some(InsertState.Update),
            list
          )
        }
        added = true
      } else if (dp.getTimestamp > datapoint.datapoint.timestamp().toEpochMilli) {
        list.add(i, newValue(datapoint.datapoint, receivedTimestamp))
        debugger.debugIncoming(
          datapoint,
          IncomingMatch.Accepted,
          receivedTimestamp,
          Some(category),
          Some(InsertState.Append),
          list
        )
        if (dp.getPublished) {
          registry
            .counter(
              insert
                .withTags(
                  "aws.namespace",
                  category.namespace,
                  "aws.metric",
                  datapoint.metricName,
                  "state",
                  "beforePublished"
                )
            )
            .increment()
        } else {
          registry
            .counter(
              insert
                .withTags(
                  "aws.namespace",
                  category.namespace,
                  "aws.metric",
                  datapoint.metricName,
                  "state",
                  "ooo"
                )
            )
            .increment()
        }
        added = true
      }
      i += 1
    }

    if (!added) {
      // apppend
      list.add(newValue(datapoint.datapoint, receivedTimestamp))
      debugger.debugIncoming(
        datapoint,
        IncomingMatch.Accepted,
        receivedTimestamp,
        Some(category),
        Some(InsertState.Append),
        list
      )
      i = list.size() - 1
      registry
        .counter(
          insert
            .withTags(
              "aws.namespace",
              category.namespace,
              "aws.metric",
              datapoint.metricName,
              "state",
              "appended"
            )
        )
        .increment()
    }

    while (list.size() > minCacheEntries) {
      list.removeFirst()
    }

    CloudWatchCacheEntry
      .newBuilder(entry)
      .clearData()
      .addAllData(list)
      .build()
  }

  private[cloudwatch] def sendToRegistry(datapoint: FirehoseMetric): Unit = {
    publishRouter.publishToRegistry(datapoint)
  }

  private[cloudwatch] def sendToRouter(key: Any, data: Array[Byte], scrapeTimestamp: Long): Unit = {
    try {
      val entry = CloudWatchCacheEntry.parseFrom(data)
      registry
        .counter(
          scraped
            .withTags("aws.namespace", entry.getNamespace, "aws.metric", entry.getMetric)
        )
        .increment()
      if (entry.getDataCount == 0) {
        delete(key)
        registry
          .counter(
            publishEmpty
              .withTags("aws.namespace", entry.getNamespace, "aws.metric", entry.getMetric)
          )
          .increment()
        debugger.debugScrape(entry, scrapeTimestamp, ScrapeState.Empty)
        return
      }

      rules.rules.get(entry.getNamespace) match {
        case None =>
          delete(key)
          purgedNS.increment()
          debugger.debugScrape(entry, scrapeTimestamp, ScrapeState.PurgedNS)
        case Some(namespaceRules) =>
          namespaceRules.get(entry.getMetric) match {
            case None =>
              delete(key)
              purgedMetric.increment()
              debugger.debugScrape(entry, scrapeTimestamp, ScrapeState.PurgedMetric)
            case Some(categories) =>
              var matchedDimensions = false
              categories.foreach { tuple =>
                val (category, definitions) = tuple
                if (category.dimensionsMatch(toAWSDimensions(entry))) {
                  matchedDimensions = true
                  if (category.filter.isDefined && category.filter.get.matches(toTagMap(entry))) {
                    delete(key)
                    purgedQuery.increment()
                    debugger.debugScrape(entry, scrapeTimestamp, ScrapeState.PurgedFilter)
                  } else {
                    val (idx, updatedEntry) = getPublishPoint(entry, scrapeTimestamp, category)
                    if (idx >= 0) {
                      if (idx >= updatedEntry.getDataCount) {
                        registry
                          .counter(
                            publishEx.withTags(
                              "aws.namespace",
                              entry.getNamespace,
                              "aws.metric",
                              entry.getMetric,
                              "ex",
                              "InvalidIndex"
                            )
                          )
                          .increment()
                        logger.warn(
                          s"bad idx ${idx} vs count ${entry.getDataCount} for ${entry.getNamespace} ${entry.getMetric}"
                        )
                        return
                      }
                      val current = Some(toAWSDatapoint(entry.getData(idx), entry.getUnit))
                      val prev =
                        if (idx > 0) Some(toAWSDatapoint(entry.getData(idx - 1), entry.getUnit))
                        else None
                      definitions.foreach { d =>
                        val metric = MetricData(
                          MetricMetadata(category, d, toAWSDimensions(entry)),
                          prev,
                          current,
                          if (prev.isDefined)
                            Some(Instant.ofEpochMilli(prev.get.timestamp().toEpochMilli))
                          else None
                        )

                        val atlasDp = toAtlasDatapoint(metric, scrapeTimestamp, category.period)
                        if (!atlasDp.value.isNaN) {
                          publishRouter.publish(atlasDp)
                        }
                      }
                    }

                    // Saves performing a deep equality check.
                    if (System.identityHashCode(entry) != System.identityHashCode(updatedEntry)) {
                      registry
                        .counter(
                          cacheUpdates.withTags(
                            "aws.namespace",
                            entry.getNamespace,
                            "aws.metric",
                            entry.getMetric
                          )
                        )
                        .increment()
                      updateCache(key, entry, updatedEntry, expSeconds(category.period))
                    }
                  }
                }
              }

              if (!matchedDimensions) {
                delete(key)
                purgedTags.increment()
                debugger.debugScrape(entry, scrapeTimestamp, ScrapeState.PurgedTag)
              }
          }
      }
    } catch {
      case ex: Exception =>
        registry
          .counter(publishEx.withTags("ex", ex.getClass.getSimpleName))
          .increment()
        logger.error(s"Unexpected exception publishing from key: ${key}", ex)
    }
  }

  /**
    * Determines the most appropriate value to publish based on the previously published offset,
    * the latest updates, etc.
    * There is a hysteresis built in that will only let it move one step forward or back in time
    * based on updates. This is to avoid jumping around in time due to delays.
    *
    * @param cache
    * @param scrapeTimestamp normalized to minute in millis
    * @return
    */
  private[cloudwatch] def getPublishPoint(
    cache: CloudWatchCacheEntry,
    scrapeTimestamp: Long,
    category: MetricCategory
  ): (Int, CloudWatchCacheEntry) = {
    var updated: CloudWatchCacheEntry = cache
    val stp = category.period * 1000
    val cutoff = stp + (stp / 2)
    val graceCutoff =
      stp * (if (category.graceOverride >= 0) category.graceOverride else gracePeriod)
    var idx = cache.getDataCount - 1
    // find the oldest non-published value
    var break = false
    var entry: CloudWatchValue = null
    var delta = 0L
    val needTwoValues = category.hasMonotonic

    // find the oldest non-published value. May pick a published value for periods over 1m.
    while (!break && idx >= 0) {
      entry = cache.getData(idx)
      delta = scrapeTimestamp - entry.getUpdateTimestamp
      if (!entry.getPublished) {
        if (delta > graceCutoff) {
          if (delta <= (graceCutoff + stp) && !needTwoValues) {
            // only record recent misses.
            registry
              .distributionSummary(
                unpublished
                  .withTags("aws.namespace", cache.getNamespace, "aws.metric", cache.getMetric)
              )
              .record(delta)
          }
          idx = -1
        } else {
          // look back to see if we can use the previous value or not.
          if (idx - 1 >= 0) {
            val prev = cache.getData(idx - 1)
            val prevDelta = scrapeTimestamp - prev.getUpdateTimestamp
            if (prev.getPublished || prevDelta > graceCutoff) {
              break = true
            } else {
              idx -= 1
            }
          } else {
            // we're at the start of the cache so we have to use what we got.
            break = true
          }
        }
      } else {
        // don't bother progressing if we've hit a published value.
        break = true
      }
    }

    if (needTwoValues) {
      if (
        idx >= 0 &&
        cache.getDataCount >= 2 &&
        idx + 1 < cache.getDataCount
      ) {
        idx += 1
        // validate the previous is within step. Otherwise not particularly useful
        val prev = cache.getData(idx - 1)
        val current = cache.getData(idx)
        if (current.getTimestamp - prev.getTimestamp > stp) {
          registry
            .counter(
              invalid.withTags(
                "aws.namespace",
                cache.getNamespace,
                "aws.metric",
                cache.getMetric,
                "reason",
                "needTwoValues"
              )
            )
            .increment()
          idx = -1
        }
      } else {
        registry
          .counter(
            invalid.withTags(
              "aws.namespace",
              cache.getNamespace,
              "aws.metric",
              cache.getMetric,
              "reason",
              "needTwoValues"
            )
          )
          .increment()
        idx = -1
      }
    }

    if (idx >= 0) {
      val entry = cache.getData(idx)
      delta = scrapeTimestamp - entry.getUpdateTimestamp
      if (entry.getPublished) {
        if (stp <= 60_000) {
          // we landed on the most recent 1m value but we may not have received anything lately.
          if (delta <= cutoff) {
            // shouldn't happen
            registry
              .distributionSummary(
                invalid.withTags(
                  "aws.namespace",
                  cache.getNamespace,
                  "aws.metric",
                  cache.getMetric,
                  "reason",
                  "alreadyPublished"
                )
              )
              .record(delta)
          }
          idx = -1
        } else if (delta > cutoff) {
          // expired
          idx = -1
        } else {
          // within wide (5m+) period so re-publish
          registry
            .distributionSummary(
              published.withTags(
                "aws.namespace",
                cache.getNamespace,
                "aws.metric",
                cache.getMetric,
                "state",
                "republished"
              )
            )
            .record(delta)
          val offset = cache.getDataCount - 1 - idx
          registry
            .distributionSummary(
              indexOffset.withTags(
                "aws.namespace",
                cache.getNamespace,
                "aws.metric",
                cache.getMetric,
                "period",
                category.period.toString
              )
            )
            .record(offset)
          val wallDelta = scrapeTimestamp - entry.getTimestamp
          registry
            .distributionSummary(
              wallOffset.withTags(
                "aws.namespace",
                cache.getNamespace,
                "aws.metric",
                cache.getMetric,
                "period",
                category.period.toString
              )
            )
            .record(wallDelta)
        }
      } else {
        // rebuild and set the published flag
        val list = new util.LinkedList[CloudWatchValue](cache.getDataList)
        val setPub = list.get(idx).toBuilder.setPublished(true).build()
        list.set(idx, setPub)
        updated = CloudWatchCacheEntry.newBuilder(cache).clearData().addAllData(list).build()

        val offset = cache.getDataCount - 1 - idx
        val delta = scrapeTimestamp - entry.getUpdateTimestamp
        if (delta > cutoff) {
          registry
            .distributionSummary(
              published.withTags(
                "aws.namespace",
                cache.getNamespace,
                "aws.metric",
                cache.getMetric,
                "state",
                "grace"
              )
            )
            .record(delta)
        } else if (delta <= cutoff) {
          registry
            .distributionSummary(
              published.withTags(
                "aws.namespace",
                cache.getNamespace,
                "aws.metric",
                cache.getMetric,
                "state",
                "current"
              )
            )
            .record(delta)
        }
        registry
          .distributionSummary(
            indexOffset.withTags(
              "aws.namespace",
              cache.getNamespace,
              "aws.metric",
              cache.getMetric,
              "period",
              category.period.toString
            )
          )
          .record(offset)
        val wallDelta = scrapeTimestamp - entry.getTimestamp
        registry
          .distributionSummary(
            wallOffset.withTags(
              "aws.namespace",
              cache.getNamespace,
              "aws.metric",
              cache.getMetric,
              "period",
              category.period.toString
            )
          )
          .record(wallDelta)

        // check the prev value (if present) to see if we may have skipped one
        if (idx - 1 >= 0) {
          val prev = cache.getData(idx - 1)
          if (!prev.getPublished) {
            val prevDelta = scrapeTimestamp - prev.getUpdateTimestamp
            if (prevDelta <= (graceCutoff + stp) && !needTwoValues) {
              registry
                .distributionSummary(
                  unpublished
                    .withTags("aws.namespace", cache.getNamespace, "aws.metric", cache.getMetric)
                )
                .record(prevDelta)
            }
          }
        }
      }
    }
    (idx, updated)
  }

  private[cloudwatch] def toAtlasDatapoint(
    metric: MetricData,
    timestamp: Long,
    step: Int
  ): AtlasDatapoint = {
    val definition = metric.meta.definition
    val ts = tagger(metric.meta.dimensions) ++ definition.tags + ("name" ->
      // TODO - clean this out once tests are finished
      // (if (testMode) s"TEST.${definition.alias}" else definition.alias))
      definition.alias)
    val newValue =
      definition.conversion(metric.meta, metric.datapoint(Instant.ofEpochMilli(timestamp)))
    // NOTE - the polling CW code uses now for the timestamp, likely for LWC. BUT data could be off by
    // minutes potentially. And it didn't account for the offset value as far as I could tell. Now we'll at least
    // use the offset value.
    new AtlasDatapoint(ts, timestamp, newValue)
  }

  def expSeconds(step: Int): Int = {
    val interval = step * 1000
    val intervals = minCacheEntries
    interval * intervals
  }
}

object CloudWatchMetricsProcessor {

  /**
    * Snaps the timestamp to the top of the period.
    *
    * @param ts
    *     The unix epoch timestamp in milliseconds.
    * @param period
    *     The period to snap to in seconds (as the configs are in seconds)
    * @return
    *     The adjusted timestamp in epoch millis
    */
  private[cloudwatch] def normalize(ts: Long, period: Int): Long = ts - (ts % (period * 1000))

  /**
    * Generates a new Protobuf cache entry from the given firehose data point and category. The data list will only
    * contain the given data point.
    *
    * @param datapoint
    *     The non-null datapoint to encode.
    * @param category
    *     The non-null category to pull a Unit from.
    * @return
    *     A non-null cache entry.
    */
  private[cloudwatch] def newCacheEntry(
    datapoint: FirehoseMetric,
    category: MetricCategory,
    receivedTimestamp: Long
  ): CloudWatchCacheEntry = {
    val dp = datapoint.datapoint
    CloudWatchCacheEntry
      .newBuilder()
      .setNamespace(datapoint.namespace)
      .setMetric(datapoint.metricName)
      .setUnit(dp.unitAsString())
      .addAllDimensions(datapoint.dimensions.map { d =>
        CloudWatchDimension
          .newBuilder()
          .setName(d.name())
          .setValue(d.value())
          .build()
      }.asJava)
      .addData(
        CloudWatchValue
          .newBuilder()
          .setTimestamp(dp.timestamp().toEpochMilli)
          .setUpdateTimestamp(receivedTimestamp)
          .setSum(dp.sum())
          .setMin(dp.minimum())
          .setMax(dp.maximum())
          .setCount(dp.sampleCount())
      )
      .build()
  }

  /**
    * Merges the two cache entries together. Publishing flags set to true always win.
    * @param a
    *     The first value to merge.
    * @param b
    *     The second value to merge.
    * @return
    *     The merged entry.
    */
  private[cloudwatch] def merge(
    a: CloudWatchCacheEntry,
    b: CloudWatchCacheEntry
  ): CloudWatchCacheEntry = {
    val aData = a.getDataList
    val bData = b.getDataList
    val data = new util.LinkedList[CloudWatchValue]()
    var ai = 0
    var bi = 0
    while (ai < aData.size() || bi < bData.size()) {
      if (ai >= aData.size()) {
        data.add(bData.get(bi))
        bi += 1
      } else if (bi >= bData.size()) {
        data.add(aData.get(ai))
        ai += 1
      } else {
        val aTs = aData.get(ai).getTimestamp
        val bTs = bData.get(bi).getTimestamp
        if (aTs < bTs) {
          data.add(aData.get(ai))
          ai += 1
        } else if (aTs > bTs) {
          data.add(bData.get(bi))
          bi += 1
        } else {
          // only diff should be the published bit
          if (aData.get(ai).getPublished) {
            data.add(aData.get(ai))
          } else {
            data.add(bData.get(bi))
          }
          ai += 1
          bi += 1
        }
      }
    }
    CloudWatchCacheEntry
      .newBuilder(a)
      .clearData()
      .addAllData(data)
      .build()
  }

  /**
    * Encodes a new Protobuf value.
    * @param dp
    *     The non-null CW data point to encode.
    * @param now
    *     The wall clock timestamp used to record when the value was updated.
    * @return
    *     A non-null cloud watch value.
    */
  private[cloudwatch] def newValue(dp: Datapoint, now: Long): CloudWatchValue = {
    CloudWatchValue
      .newBuilder()
      .setTimestamp(dp.timestamp().toEpochMilli)
      .setUpdateTimestamp(now)
      .setSum(dp.sum())
      .setMin(dp.minimum())
      .setMax(dp.maximum())
      .setCount(dp.sampleCount())
      .build()
  }

  /**
    * Converts the Protobuf value to an AWS data point.
    *
    * @param dp
    *     The non-null cloud watch data point.
    * @param unit
    *     The non-null unit from the cloud watch cache entry.
    * @return
    *     A non-null data point.
    */
  private[cloudwatch] def toAWSDatapoint(dp: CloudWatchValue, unit: String): Datapoint = {
    Datapoint
      .builder()
      .timestamp(Instant.ofEpochMilli(dp.getTimestamp))
      .unit(unit)
      .sum(dp.getSum)
      .maximum(dp.getMax)
      .minimum(dp.getMin)
      .sampleCount(dp.getCount)
      .build()
  }

  /**
    * Converts the given Protobuf entry's dimensions into a list of AWS dimensions.
    *
    * @param entry
    *     The non-null entry to convert.
    * @return
    *     A non-null, potentially empty, list of dimensions.
    */
  private[cloudwatch] def toAWSDimensions(entry: CloudWatchCacheEntry): List[Dimension] = {
    entry.getDimensionsList.asScala.map { d =>
      Dimension
        .builder()
        .name(d.getName)
        .value(d.getValue)
        .build()
    }.toList
  }

  /**
    * Converts the dimensions from the Protobuf entry to a sorted tag map to use in evaluating against a
    * Query filter from a [MetricCategory] config. Note that the `name` and `aws.namespace` tags are populated per
    * Atlas standards.
    *
    * @param entry
    *     The non-null entry to encode.
    * @return
    *     A non-null map with at least the `name` and `aws.namespace` tags.
    */
  private[cloudwatch] def toTagMap(entry: CloudWatchCacheEntry): Map[String, String] = {
    SmallHashMap(
      entry.getDimensionsCount + 2,
      entry.getDimensionsList.asScala
        .map(d => d.getName -> d.getValue)
        .append("name" -> entry.getMetric)
        .append("aws.namespace" -> entry.getNamespace.replaceAll("/", "_"))
        .iterator
    )
  }

  /**
    * Converts the dimensions from the firehose datapoint to a sorted tag map to use in evaluating against a
    * Query filter from a [MetricCategory] config. Note that the `name` and `aws.namespace` tags are populated per
    * Atlas standards.
    *
    * @param entry
    * The non-null entry to encode.
    * @return
    * A non-null map with at least the `name` and `aws.namespace` tags.
    */
  private[cloudwatch] def toTagMap(datapoint: FirehoseMetric): Map[String, String] = {
    SmallHashMap(
      datapoint.dimensions.size + 2,
      datapoint.dimensions
        .map(d => d.name() -> d.value())
        .appended("name" -> datapoint.metricName)
        .appended("aws.namespace" -> datapoint.namespace.replaceAll("/", "_"))
        .iterator
    )
  }

  /**
    * Converts the dimensions from the CloudWatch Metric to a sorted tag map to use in evaluating against a
    * Query filter from a [MetricCategory] config. Note that the `name` and `aws.namespace` tags are populated per
    * Atlas standards.
    *
    * @param metric
    *     The non-null entry to encode.
    * @return
    *     A non-null map with at least the `name` and `aws.namespace` tags.
    */
  private[cloudwatch] def toTagMap(metric: Metric): Map[String, String] = {
    SmallHashMap(
      metric.dimensions().size() + 2,
      metric
        .dimensions()
        .asScala
        .map(d => d.name() -> d.value())
        .append("name" -> metric.metricName())
        .append("aws.namespace" -> metric.namespace().replaceAll("/", "_"))
        .iterator
    )
  }

}
