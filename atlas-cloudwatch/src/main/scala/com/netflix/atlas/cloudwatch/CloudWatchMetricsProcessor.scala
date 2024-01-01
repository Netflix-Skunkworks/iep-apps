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
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.expirationSeconds
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

  /** The number of data points overwriting a duplicate value. */
  private[cloudwatch] val dupes = registry.createId("atlas.cloudwatch.datapoints.dupes")

  /** The number of data points dropped due to being older than the period count. */
  private[cloudwatch] val droppedOld =
    registry.createId("atlas.cloudwatch.datapoints.dropped", "reason", "tooOld")

  /** The number of data points arriving out of order. Data is still kept and processed. */
  private[cloudwatch] val outOfOrder = registry.createId("atlas.cloudwatch.datapoints.ooo")

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

  /** The number of data points still published even though their timestamp was for at least one step
    *  later than the expected publishing time. This is usually related to end-period-offsets but may be
    *  clock issues. */
  private[cloudwatch] val publishFuture = registry.createId("atlas.cloudwatch.publish.future")
  private[cloudwatch] val publishAtOffset = registry.createId("atlas.cloudwatch.publish.atOffset")
  private[cloudwatch] val staleAge = registry.createId("atlas.cloudwatch.publish.stale.age")
  private[cloudwatch] val staleWTimeout = registry.createId("atlas.cloudwatch.publish.inTimeout")
  private[cloudwatch] val publishStep = registry.createId("atlas.cloudwatch.publish.step")

  private val step = config.getDuration("atlas.cloudwatch.step").getSeconds.toInt
  private val publishDelay = config.getDuration("atlas.cloudwatch.publishOffset").getSeconds.toInt
  // TODO - temp and removable once we're happy with the results.
  private val testMode = config.getBoolean("atlas.cloudwatch.testMode")

  // ctor
  {
    val delay = {
      val now = System.currentTimeMillis()
      val topOfMinute = normalize(now, step)
      ((step * 1000) - (now - topOfMinute)) + (publishDelay * 1000)
    }
    logger.info(s"Starting publishing in ${delay / 1000.0} seconds.")
    logger.info(s"Publishing with in test mode: ${testMode}")
    system.scheduler.scheduleAtFixedRate(
      FiniteDuration.apply(delay, TimeUnit.MILLISECONDS),
      FiniteDuration.apply(step, TimeUnit.SECONDS)
    )(() => {
      try {
        publish(normalize(System.currentTimeMillis(), step))
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
              registry.counter(filteredMetric.withTag("aws.namespace", dp.namespace)).increment()
              debugger.debugIncoming(dp, IncomingMatch.DroppedMetric, receivedTimestamp)
            case Some(tuple) =>
              val (category, _) = tuple
              if (!category.dimensionsMatch(dp.dimensions)) {
                registry.counter(filteredTags.withTag("aws.namespace", dp.namespace)).increment()
                debugger.debugIncoming(
                  dp,
                  IncomingMatch.DroppedTag,
                  receivedTimestamp,
                  Some(category)
                )
              } else if (category.filter.isDefined && tuple._1.filter.get.matches(toTagMap(dp))) {
                registry.counter(filteredQuery.withTag("aws.namespace", dp.namespace)).increment()
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
    * Called by the scheduler to publish data accumulated in the cache. Exposed for unit testing.
    */
  protected[cloudwatch] def publish(now: Long): Future[NotUsed]

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
    CloudWatchCacheEntry
      .newBuilder(entry)
      .clearData()
      .addAllData(insert(datapoint, category, entry.getDataList, receivedTimestamp))
      .build()
  }

  private[cloudwatch] def insert(
    datapoint: FirehoseMetric,
    category: MetricCategory,
    data: java.util.List[CloudWatchValue],
    receivedTimestamp: Long
  ): java.util.List[CloudWatchValue] = {
    val exp = (expirationSeconds(category) - category.period) * 1000
    // this is the oldest value to keep. Anything older, we kick out.
    val oldest = normalize(receivedTimestamp, 60) - exp

    val filtered =
      new util.LinkedList[CloudWatchValue](data.asScala.filter(_.getTimestamp >= oldest).asJava)
    val ts = normalize(datapoint.datapoint.timestamp().toEpochMilli, 60)
    if (ts < oldest) {
      registry.counter(droppedOld.withTag("aws.namespace", category.namespace)).increment()
      debugger.debugIncoming(datapoint, IncomingMatch.DroppedOld, receivedTimestamp, Some(category))
    } else {
      if (filtered.isEmpty) {
        filtered.add(newValue(ts, datapoint.datapoint))
        debugger.debugIncoming(
          datapoint,
          IncomingMatch.Accepted,
          receivedTimestamp,
          Some(category),
          Some(InsertState.Append),
          filtered
        )
      } else if (filtered.get(0).getTimestamp > ts) {
        // prepend
        filtered.add(0, newValue(ts, datapoint.datapoint))
        registry.counter(outOfOrder.withTag("aws.namespace", category.namespace)).increment()
        debugger.debugIncoming(
          datapoint,
          IncomingMatch.Accepted,
          receivedTimestamp,
          Some(category),
          Some(InsertState.OOO),
          filtered
        )
      } else {
        // see if we need to overwrite or insert
        var added = false
        var i = 0
        while (i < filtered.size() && !added) {
          val dp = filtered.get(i)
          if (dp.getTimestamp == ts) {
            registry.counter(dupes.withTag("aws.namespace", category.namespace)).increment()
            filtered.set(i, newValue(ts, datapoint.datapoint))
            debugger.debugIncoming(
              datapoint,
              IncomingMatch.Accepted,
              receivedTimestamp,
              Some(category),
              Some(InsertState.Update),
              filtered
            )
            added = true
          } else if (dp.getTimestamp > ts) {
            filtered.add(i, newValue(ts, datapoint.datapoint))
            registry.counter(outOfOrder.withTag("aws.namespace", category.namespace)).increment()
            debugger.debugIncoming(
              datapoint,
              IncomingMatch.Accepted,
              receivedTimestamp,
              Some(category),
              Some(InsertState.Append),
              filtered
            )
            added = true
          }
          i += 1
        }

        if (!added) {
          // apppend
          filtered.add(newValue(ts, datapoint.datapoint))
          debugger.debugIncoming(
            datapoint,
            IncomingMatch.Accepted,
            receivedTimestamp,
            Some(category),
            Some(InsertState.Append),
            filtered
          )
        }
      }
    }

    filtered
  }

  private[cloudwatch] def sendToRouter(key: Any, data: Array[Byte], timestamp: Long): Unit = {
    val entry = CloudWatchCacheEntry.parseFrom(data)
    if (entry.getDataCount == 0) {
      delete(key)
      registry.counter(publishEmpty.withTag("aws.namespace", entry.getNamespace)).increment()
      debugger.debugScrape(entry, timestamp, ScrapeState.Empty)
      return
    }

    rules.rules.get(entry.getNamespace) match {
      case None =>
        delete(key)
        purgedNS.increment()
        debugger.debugScrape(entry, timestamp, ScrapeState.PurgedNS)
      case Some(namespaceRules) =>
        namespaceRules.get(entry.getMetric) match {
          case None =>
            delete(key)
            purgedMetric.increment()
            debugger.debugScrape(entry, timestamp, ScrapeState.PurgedMetric)
          case Some(tuple) =>
            val (category, definitions) = tuple
            if (!category.dimensionsMatch(toAWSDimensions(entry))) {
              delete(key)
              purgedTags.increment()
              debugger.debugScrape(entry, timestamp, ScrapeState.PurgedTag)
            } else if (category.filter.isDefined && category.filter.get.matches(toTagMap(entry))) {
              delete(key)
              purgedQuery.increment()
              debugger.debugScrape(entry, timestamp, ScrapeState.PurgedFilter)
            } else {
              var i = 0
              val offsetTimestamp =
                timestamp - ((category.period * category.endPeriodOffset) * 1000)
              while (i < entry.getDataCount && entry.getData(i).getTimestamp < offsetTimestamp) {
                i += 1
              }

              val tuple = if (i >= entry.getDataCount) {
                val dp = entry.getData(entry.getDataCount - 1)
                val delta = (timestamp - dp.getTimestamp) / 1000
                registry
                  .distributionSummary(staleAge.withTag("aws.namespace", category.namespace))
                  .record(delta)
                category.timeout match {
                  case Some(timeout) =>
                    // nothing satisfies the offset so we check for timeouts and only publish if there
                    // was a value within the timeout period
                    if (timestamp - dp.getTimestamp < timeout.toMillis) {
                      registry
                        .counter(staleWTimeout.withTag("aws.namespace", category.namespace))
                        .increment()
                      debugger.debugScrape(
                        entry,
                        timestamp,
                        ScrapeState.InTimeout,
                        offsetTimestamp,
                        Some(dp),
                        Some(category)
                      )
                      Some(toAWSDatapoint(dp, entry.getUnit)) -> None
                    } else {
                      debugger.debugScrape(
                        entry,
                        timestamp,
                        ScrapeState.Stale,
                        offsetTimestamp,
                        Some(dp),
                        Some(category)
                      )
                      None -> None
                    }
                  case None =>
                    debugger.debugScrape(
                      entry,
                      timestamp,
                      ScrapeState.Stale,
                      offsetTimestamp,
                      Some(dp),
                      Some(category)
                    )
                    None -> None
                }
              } else {
                val dp = entry.getData(i)
                if (dp.getTimestamp > offsetTimestamp) {
                  registry
                    .distributionSummary(
                      publishFuture.withTags("aws.namespace", category.namespace)
                    )
                    .record(dp.getTimestamp - offsetTimestamp)
                  debugger.debugScrape(
                    entry,
                    timestamp,
                    ScrapeState.Future,
                    offsetTimestamp,
                    Some(dp),
                    Some(category)
                  )
                } else {
                  registry
                    .counter(publishAtOffset.withTags("aws.namespace", category.namespace))
                    .increment()
                  debugger.debugScrape(
                    entry,
                    timestamp,
                    ScrapeState.OnTime,
                    offsetTimestamp,
                    Some(dp),
                    Some(category)
                  )
                }

                val p = if (i > 0) {
                  val pdp = entry.getData(i - 1)
                  val delta = (dp.getTimestamp - pdp.getTimestamp) / 1000
                  registry
                    .distributionSummary(publishStep.withTags("aws.namespace", category.namespace))
                    .record(delta)
                  Some(toAWSDatapoint(pdp, entry.getUnit))
                } else None

                p -> Some(toAWSDatapoint(dp, entry.getUnit))
              }

              tuple match {
                case (p: Option[Datapoint], c: Option[Datapoint]) =>
                  definitions.foreach { d =>
                    val metric = MetricData(
                      MetricMetadata(category, d, toAWSDimensions(entry)),
                      p,
                      c,
                      if (p.isDefined) Some(Instant.ofEpochMilli(p.get.timestamp().toEpochMilli))
                      else None
                    )
                    val atlasDp = toAtlasDatapoint(metric, timestamp)
                    if (!atlasDp.value.isNaN) {
                      publishRouter.publish(atlasDp)
                    }
                  }
                case (_, _) => // no-op
              }
            }
        }
    }
  }

  private[cloudwatch] def toAtlasDatapoint(metric: MetricData, timestamp: Long): AtlasDatapoint = {
    val definition = metric.meta.definition
    val ts = tagger(metric.meta.dimensions) ++ definition.tags + ("name" ->
      // TODO - clean this out once tests are finished
      (if (testMode) s"TEST.${definition.alias}" else definition.alias))
    val newValue =
      definition.conversion(metric.meta, metric.datapoint(Instant.ofEpochMilli(timestamp)))
    // NOTE - the polling CW code uses now for the timestamp, likely for LWC. BUT data could be off by
    // minutes potentially. And it didn't account for the offset value as far as I could tell. Now we'll at least
    // use the offset value.
    new AtlasDatapoint(ts, timestamp, newValue)
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
    *     The adjusted timestamp.
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
    category: MetricCategory
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
          .setTimestamp(normalize(dp.timestamp().toEpochMilli, category.period))
          .setSum(dp.sum())
          .setMin(dp.minimum())
          .setMax(dp.maximum())
          .setCount(dp.sampleCount())
      )
      .build()
  }

  /**
    * Encodes a new Protobuf value.
    * @param ts
    *     The timestamp of the value in unix epoch milliseconds.
    * @param dp
    *     The non-null CW data point to encode.
    * @return
    *     A non-null cloud watch value.
    */
  private[cloudwatch] def newValue(ts: Long, dp: Datapoint): CloudWatchValue = {
    CloudWatchValue
      .newBuilder()
      .setTimestamp(ts)
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

  /**
    * Computes the expiration time for a value based on the [MetricCategory] configuration. It accounts for monotonic
    * counters by adding a period to the period count, then returns the max of either the period * period count or
    * the timeout value if set.
    *
    * @param category
    *     The non-null category to pull settings from.
    * @return
    *     How long, in seconds, before expiration.
    */
  private[cloudwatch] def expirationSeconds(category: MetricCategory): Long = {
    var periodCount = Math.max(category.periodCount, category.endPeriodOffset)
    if (category.hasMonotonic) periodCount += 1
    val periodExpiration = periodCount * category.period

    if (category.timeout.isDefined) {
      periodExpiration + (category.timeout.get.getSeconds * 2)
    } else {
      periodExpiration
    }
  }
}
