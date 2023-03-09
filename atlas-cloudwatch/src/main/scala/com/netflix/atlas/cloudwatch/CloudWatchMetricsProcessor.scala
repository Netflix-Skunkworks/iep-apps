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

import akka.actor.ActorSystem
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

import java.time.Instant
import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
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
  *
  * @param config
  *     Non-null config to pull settings from.
  * @param registry
  *     Non-null registry to report metrics.
  * @param rules
  *     Non-null rules set to use for matching incoming data.
  * @param system
  *     The Akka system we use for scheduling.
  */
abstract class CloudWatchMetricsProcessor(
  config: Config,
  registry: Registry,
  rules: CloudWatchRules,
  tagger: Tagger,
  publishRouter: PublishRouter
)(implicit val system: ActorSystem)
    extends StrictLogging {

  /** The number of data points received by the processor from cloud watch.  */
  private[cloudwatch] val received = registry.counter("atlas.cloudwatch.datapoints.received")

  /** The number of data points filtered out before caching due to not having a namespace config. */
  private[cloudwatch] val filteredNS =
    registry.counter("atlas.cloudwatch.datapoints.filtered", "reason", "namespace")

  /** The number of data points filtered out before caching due to not having a metric config. */
  private[cloudwatch] val filteredMetric =
    registry.counter("atlas.cloudwatch.datapoints.filtered", "reason", "metric")

  /** The number of data points filtered out before caching due to missing or extraneous tags not definied in the category. */
  private[cloudwatch] val filteredTags =
    registry.counter("atlas.cloudwatch.datapoints.filtered", "reason", "tags")

  /** The number of data points filtered out before caching due to a query filter. */
  private[cloudwatch] val filteredQuery =
    registry.counter("atlas.cloudwatch.datapoints.filtered", "reason", "query")

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

  /** The number of data points still published even though their timestamp was for a time later than the expected
    * publishing time. This is usually related to end-period-offsets but may be clock issues. */
  private[cloudwatch] val publishFuture = registry.createId("atlas.cloudwatch.publish.future")

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
        case None => filteredNS.increment()
        case Some(namespaceRules) =>
          namespaceRules.get(dp.metricName) match {
            case None => filteredMetric.increment()
            case Some(tuple) =>
              if (!tuple._1.dimensionsMatch(dp.dimensions)) {
                filteredTags.increment()
              } else if (tuple._1.filter.isDefined && tuple._1.filter.get.matches(toTagMap(dp))) {
                filteredQuery.increment()
              } else {
                // finally passed the checks.
                updateCache(dp, tuple._1, receivedTimestamp)
              }

              val delay = receivedTimestamp - dp.datapoint.timestamp().toEpochMilli
              registry
                .distributionSummary(
                  "atlas.cloudwatch.namespace.delay",
                  "namespace",
                  tuple._1.namespace,
                  "metric",
                  dp.metricName
                )
                .record(delay)
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
  protected def updateCache(
    datapoint: FirehoseMetric,
    category: MetricCategory,
    receivedTimestamp: Long
  ): Unit

  /**
    * Called by the scheduler to publish data accumulated in the cache. Exposed for unit testing.
    */
  protected[cloudwatch] def publish(now: Long): Future[Unit]

  /**
    * Removes the given entry from the cache. This is used to purge entries that are no longer valid due to a config
    * change or error.
    *
    * @param key
    *     The non-null key to delete. It's an `Any` for now since the key could be in any format.
    */
  protected[cloudwatch] def delete(key: Any): Unit

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
    val ts = normalize(datapoint.datapoint.timestamp().toEpochMilli, category.period)
    if (ts < oldest) {
      logger.warn(
        s"Dropping data for namespace {} and metric {} that was older than the retention period of ${exp / 1000}s. {}s over.",
        category.namespace,
        datapoint.metricName,
        (oldest - ts) / 1000.0
      )
      registry.counter(droppedOld.withTag("namespace", category.namespace)).increment()
    } else {
      if (filtered.isEmpty) {
        filtered.add(newValue(ts, datapoint.datapoint))
      } else if (filtered.get(0).getTimestamp > ts) {
        // prepend
        filtered.add(0, newValue(ts, datapoint.datapoint))
        registry.counter(outOfOrder.withTag("namespace", category.namespace)).increment()
      } else {
        // see if we need to overwrite or insert
        var added = false
        var i = 0
        while (i < filtered.size() && !added) {
          val dp = filtered.get(i)
          if (dp.getTimestamp == ts) {
            logger.debug(
              s"Duplicate value for namespace {} and metric {}",
              category.namespace,
              datapoint.metricName
            )
            registry.counter(dupes.withTag("namespace", category.namespace)).increment()
            filtered.set(i, newValue(ts, datapoint.datapoint))
            added = true
          } else if (dp.getTimestamp > ts) {
            filtered.add(i, newValue(ts, datapoint.datapoint))
            registry.counter(outOfOrder.withTag("namespace", category.namespace)).increment()
            added = true
          }
          i += 1
        }

        if (!added) {
          // apppend
          filtered.add(newValue(ts, datapoint.datapoint))
        }
      }
    }

    filtered
  }

  private[cloudwatch] def sendToRouter(key: Any, data: Array[Byte], timestamp: Long): Unit = {
    val entry = CloudWatchCacheEntry.parseFrom(data)
    if (entry.getDataCount == 0) {
      delete(key)
      registry.counter(publishEmpty.withTag("namespace", entry.getNamespace)).increment()
      return
    }

    rules.rules.get(entry.getNamespace) match {
      case None =>
        delete(key)
        purgedNS.increment()
      case Some(namespaceRules) =>
        namespaceRules.get(entry.getMetric) match {
          case None =>
            delete(key)
            purgedMetric.increment()
          case Some(tuple) =>
            val (category, definitions) = tuple
            if (!category.dimensionsMatch(toAWSDimensions(entry))) {
              delete(key)
              purgedTags.increment()
            } else if (category.filter.isDefined && category.filter.get.matches(toTagMap(entry))) {
              delete(key)
              purgedQuery.increment()
            } else {
              var i = 0
              val offsetTimestamp =
                timestamp - ((category.period * category.endPeriodOffset) * 1000)
              while (i < entry.getDataCount && entry.getData(i).getTimestamp < offsetTimestamp) {
                i += 1
              }
              if (i == entry.getDataCount) {
                i = entry.getDataCount - 1
              }
              // TODO - it's possible that a value in the future relative to the lookup timestamp is returned here, e.g.
              // if the entry at index 0 is greater than timestamp. In that case, do we not report? OR report the value?
              if (entry.getData(i).getTimestamp > offsetTimestamp) {
                registry.counter(publishFuture.withTag("namespace", category.namespace)).increment()
              }
              // TODO - there are some configs where the timeout is less than the period * period count, e.g. Neptune.
              // if we _don't_ want the last available value to keep posting all the time, then we need to incorporate
              // that timeout here. Otherwise, e.g. for a value posted once a day, we'll keep posting the daily value until
              // we get a new one.
              val cur = toAWSDatapoint(entry.getData(i), entry.getUnit)
              val prev = if (i > 0) {
                Some(toAWSDatapoint(entry.getData(i - 1), entry.getUnit))
              } else None
              definitions.foreach { d =>
                val metric = MetricData(
                  MetricMetadata(category, d, toAWSDimensions(entry)),
                  prev,
                  Some(cur),
                  if (prev.isDefined) Some(Instant.ofEpochMilli(prev.get.timestamp().toEpochMilli))
                  else None
                )
                val atlasDp = toAtlasDatapoint(metric, timestamp)
                publishRouter.publish(atlasDp)
              }
            }
        }
    }
  }

  private[cloudwatch] def toAtlasDatapoint(
    metric: MetricData,
    receivedTimestamp: Long
  ): AtlasDatapoint = {
    val definition = metric.meta.definition
    val ts = tagger(metric.meta.dimensions) ++ definition.tags + ("name" ->
      // TODO - clean this out once tests are finished
      (if (testMode) s"TEST.${definition.alias}" else definition.alias))
    val newValue = definition.conversion(metric.meta, metric.datapoint())
    // NOTE - the polling CW code uses now for the timestamp, likely for LWC. BUT data could be off by
    // minutes potentially. And it didn't account for the offset value as far as I could tell. Now we'll at least
    // use the offset value.
    new AtlasDatapoint(ts, receivedTimestamp, newValue)
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
        .append("aws.namespace" -> entry.getNamespace)
        .iterator
    )
  }

  /**
    * Converts the dimensions from the firehose datapoint to a sorted tag map to use in evaluating against a
    * Query filter from a [MetricCategory] config. Note that the `name` and `aws.namespace` tags are populated per
    * Atlas standards.
    *
    * @return
    * A non-null map with at least the `name` and `aws.namespace` tags.
    */
  private[cloudwatch] def toTagMap(datapoint: FirehoseMetric): Map[String, String] = {
    SmallHashMap(
      datapoint.dimensions.size + 2,
      datapoint.dimensions
        .map(d => d.name() -> d.value())
        .appended("name" -> datapoint.metricName)
        .appended("aws.namespace" -> datapoint.namespace)
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
    val periodCount =
      if (category.hasMonotonic) category.periodCount + 1
      else category.periodCount
    val periodExpiration = periodCount * category.period

    if (category.timeout.isDefined) {
      Math.max(periodExpiration, category.timeout.get.getSeconds)
    } else {
      periodExpiration
    }
  }

}
