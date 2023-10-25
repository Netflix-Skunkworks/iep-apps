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

import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.normalize
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toTagMap
import com.netflix.atlas.cloudwatch.IncomingMatch.IncomingMatch
import com.netflix.atlas.cloudwatch.InsertState.InsertState
import com.netflix.atlas.cloudwatch.MetricCategory.parseQuery
import com.netflix.atlas.cloudwatch.ScrapeState.ScrapeState
import com.netflix.atlas.core.index.QueryIndex
import com.netflix.atlas.json.Json
import com.netflix.atlas.util.XXHasher
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Metric

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*
import scala.util.Using

class CloudWatchDebugger(
  config: Config,
  registry: Registry
) extends StrictLogging {

  private val debugTags = config.getBoolean("atlas.cloudwatch.debug.dropped.tags")
  private val debugAge = config.getBoolean("atlas.cloudwatch.debug.dropped.age")
  private val debugStale = config.getBoolean("atlas.cloudwatch.debug.scrape.stale")
  private val configRules = config.getStringList("atlas.cloudwatch.debug.rules").asScala

  private val rules: QueryIndex[Boolean] = QueryIndex.create(
    configRules
      .map(rule => QueryIndex.Entry(parseQuery(rule), true))
      .toList
  )

  private val incomingLimiter = new ConcurrentHashMap[Long, Long]
  private val outgoingLimiter = new ConcurrentHashMap[Long, Long]

  private[cloudwatch] val filteredNS = registry.createId("atlas.cloudwatch.dbg.ingest.filtered.ns")

  private[cloudwatch] val filteredMetric =
    registry.createId("atlas.cloudwatch.dbg.ingest.filtered.metric")

  private[cloudwatch] val filteredTags =
    registry.createId("atlas.cloudwatch.dbg.ingest.filtered.tags")

  private[cloudwatch] val filteredFilter =
    registry.createId("atlas.cloudwatch.dbg.ingest.filtered.filter")

  private[cloudwatch] val filteredAge =
    registry.createId("atlas.cloudwatch.dbg.ingest.filtered.age")

  private[cloudwatch] val filteredEmpty =
    registry.createId("atlas.cloudwatch.dbg.ingest.filtered.empty")
  private[cloudwatch] val accepted = registry.createId("atlas.cloudwatch.dbg.ingest.accepted")

  private[cloudwatch] val scrapeEmpty = registry.createId("atlas.cloudwatch.dbg.scrape.empty")
  private[cloudwatch] val scrapeFuture = registry.createId("atlas.cloudwatch.dbg.scrape.future")
  private[cloudwatch] val scrapeOffset = registry.createId("atlas.cloudwatch.dbg.scrape.atOffset")
  private[cloudwatch] val scrapePast = registry.createId("atlas.cloudwatch.dbg.scrape.past")

  private[cloudwatch] val scrapeWTimeout =
    registry.createId("atlas.cloudwatch.dbg.scrape.inTimeout")
  private[cloudwatch] val scrapeStep = registry.createId("atlas.cloudwatch.dbg.scrape.step")

  def debugIncoming(
    dp: FirehoseMetric,
    incomingMatch: IncomingMatch,
    timestamp: Long,
    category: Option[MetricCategory] = None,
    insertState: Option[InsertState] = None,
    cacheEntries: java.util.List[CloudWatchValue] = Collections.emptyList()
  ): Unit = {
    if (config.isEmpty) return

    if (
      !rules.matches(toTagMap(dp)) &&
      !(debugTags && incomingMatch == IncomingMatch.DroppedTag) &&
      !(debugAge && incomingMatch == IncomingMatch.DroppedOld)
    ) return

    try {
      // if we've already debugged a similar namespace metric and dimension key set. Purposely
      // not including the tag values.
      val ts = normalize(timestamp, 60)
      val hashCode = hash(dp, insertState)
      val prevReport = incomingLimiter.put(hashCode, timestamp)

      val tags = Map(
        "aws.namespace" -> dp.namespace,
        "metrics"       -> dp.metricName
      ).asJava

      incomingMatch match {
        case IncomingMatch.DroppedNS =>
          registry.counter(filteredNS.withTags(tags)).increment()
        case IncomingMatch.DroppedMetric =>
          registry.counter(filteredMetric.withTags(tags)).increment()
        case IncomingMatch.DroppedTag =>
          registry.counter(filteredTags.withTags(tags)).increment()
        case IncomingMatch.DroppedFilter =>
          registry.counter(filteredFilter.withTags(tags)).increment()
        case IncomingMatch.DroppedOld =>
          registry
            .distributionSummary(filteredAge.withTags(tags))
            .record((timestamp - dp.datapoint.timestamp().toEpochMilli).toInt / 1000)
        case IncomingMatch.Accepted =>
          registry.counter(accepted.withTags(tags)).increment()
      }
      if (prevReport == ts) return // here's our limiter

      val stream = new ByteArrayOutputStream()
      Using.resource(Json.newJsonGenerator(stream)) { json =>
        json.writeStartObject()
        json.writeStringField("state", "incoming")
        json.writeNumberField("hash", dp.xxHash)
        json.writeNumberField("rts", timestamp)
        json.writeStringField("ns", dp.namespace)
        json.writeStringField("metric", dp.metricName)
        json.writeObjectFieldStart("tags")
        dp.dimensions.foreach(d => json.writeStringField(d.name(), d.value()))
        json.writeEndObject()
        json.writeNumberField("dts", dp.datapoint.timestamp().toEpochMilli)
        json.writeNumberField(
          "ageFromNow",
          (timestamp - dp.datapoint.timestamp().toEpochMilli).toInt / 1000
        )
        json.writeNumberField("sum", dp.datapoint.sum())
        json.writeNumberField("min", dp.datapoint.minimum())
        json.writeNumberField("max", dp.datapoint.maximum())
        json.writeNumberField("count", dp.datapoint.sampleCount())
        json.writeStringField("unit", dp.datapoint.unit().toString)

        json.writeStringField("filtered", incomingMatch.toString)
        if (insertState.isDefined) {
          json.writeStringField("insert", insertState.get.toString)
        }

        if (category.isDefined) {
          val cat = category.get
          json.writeObjectFieldStart("cat")
          json.writeNumberField("p", cat.period)
          json.writeNumberField("off", cat.endPeriodOffset)
          json.writeNumberField("to", cat.timeout.getOrElse(Duration.ofSeconds(0)).getSeconds)
          json.writeArrayFieldStart("tags")
          cat.dimensions.foreach(d => json.writeString(d))
          json.writeEndArray()
          json.writeEndObject()
        }

        if (!cacheEntries.isEmpty) {
          var step = 0L
          var last = 0L
          json.writeArrayFieldStart("cached")
          for (i <- 0 until cacheEntries.size()) {
            val d = cacheEntries.get(i)
            val dts = d.getTimestamp
            json.writeStartObject()
            json.writeNumberField("ts", dts)
            json.writeNumberField("offFromWindow", (ts - dts).toInt / 1000)
            json.writeNumberField("sum", d.getSum())
            json.writeNumberField("min", d.getMin())
            json.writeNumberField("max", d.getMax)
            json.writeNumberField("count", d.getCount)
            if (last != 0) {
              step += (dts - last)
            }
            last = dts

            json.writeEndObject()
          }
          json.writeEndArray()

          if (cacheEntries.size() >= 2) {
            json.writeNumberField("avgStep", ((step / (cacheEntries.size() - 1)) / 1000).toInt)
          }
        }
        json.writeEndObject()
      }

      logger.info(stream.toString("UTF-8"))
    } catch {
      case ex: Exception => logger.error("whoops", ex)
    }
  }

  def debugPolled(
    metric: Metric,
    incomingMatch: IncomingMatch,
    timestamp: Long,
    category: MetricCategory,
    dataPoints: java.util.List[Datapoint] = Collections.emptyList()
  ): Unit = {
    if (config.isEmpty) return

    if (
      !rules.matches(toTagMap(metric)) &&
      !(debugTags && incomingMatch == IncomingMatch.DroppedTag) &&
      !(debugAge && incomingMatch == IncomingMatch.DroppedOld)
    ) return

    try {
      // if we've already debugged a similar namespace metric and dimension key set. Purposely
      // not including the tag values.
      val ts = normalize(timestamp, 60)
      val hashCode = hash(metric)
      val prevReport = incomingLimiter.put(hashCode, timestamp)

      val tags = Map(
        "aws.namespace" -> metric.namespace(),
        "metrics"       -> metric.metricName()
      ).asJava

      incomingMatch match {
        case IncomingMatch.DroppedNS =>
          registry.counter(filteredNS.withTags(tags)).increment()
        case IncomingMatch.DroppedMetric =>
          registry.counter(filteredMetric.withTags(tags)).increment()
        case IncomingMatch.DroppedTag =>
          registry.counter(filteredTags.withTags(tags)).increment()
        case IncomingMatch.DroppedFilter =>
          registry.counter(filteredFilter.withTags(tags)).increment()
        case IncomingMatch.DroppedEmpty =>
          registry.counter(filteredEmpty.withTags(tags)).increment()
        case IncomingMatch.Accepted =>
          registry.counter(accepted.withTags(tags)).increment()
      }
      if (prevReport == ts) return // here's our limiter

      val stream = new ByteArrayOutputStream()
      Using.resource(Json.newJsonGenerator(stream)) { json =>
        json.writeStartObject()
        json.writeStringField("state", "incoming")
        json.writeNumberField("hash", hashCode)
        json.writeNumberField("rts", timestamp)
        json.writeStringField("ns", metric.namespace())
        json.writeStringField("metric", metric.metricName())
        json.writeObjectFieldStart("tags")
        metric.dimensions.asScala.foreach(d => json.writeStringField(d.name(), d.value()))
        json.writeEndObject()

        json.writeStringField("filtered", incomingMatch.toString)
        json.writeObjectFieldStart("cat")
        json.writeNumberField("p", category.period)
        json.writeNumberField("off", category.endPeriodOffset)
        json.writeNumberField("to", category.timeout.getOrElse(Duration.ofSeconds(0)).getSeconds)
        json.writeArrayFieldStart("tags")
        category.dimensions.foreach(d => json.writeString(d))
        json.writeEndArray()
        json.writeEndObject()

        if (!dataPoints.isEmpty) {
          var step = 0L
          var last = 0L
          json.writeArrayFieldStart("cached")
          for (i <- 0 until dataPoints.size()) {
            val d = dataPoints.get(i)
            val dts = d.timestamp().toEpochMilli
            json.writeStartObject()
            json.writeNumberField("ts", dts)
            json.writeNumberField("offFromWindow", (ts - dts).toInt / 1000)
            json.writeNumberField("sum", d.sum())
            json.writeNumberField("min", d.minimum())
            json.writeNumberField("max", d.maximum())
            json.writeNumberField("count", d.sampleCount())
            if (last != 0) {
              step += (dts - last)
            }
            last = dts

            json.writeEndObject()
          }
          json.writeEndArray()

          if (dataPoints.size() >= 2) {
            json.writeNumberField("avgStep", ((step / (dataPoints.size() - 1)) / 1000).toInt)
          }
        }
        json.writeEndObject()
      }

      logger.info(stream.toString("UTF-8"))
    } catch {
      case ex: Exception => logger.error("whoops", ex)
    }
  }

  def debugScrape(
    cacheEntry: CloudWatchCacheEntry,
    scrapeTimestamp: Long,
    scrapeState: ScrapeState,
    offsetTimestamp: Long = 0,
    value: Option[CloudWatchValue] = None,
    category: Option[MetricCategory] = None
  ): Unit = {
    if (config.isEmpty) return

    if (!rules.matches(toTagMap(cacheEntry))) return

    try {
      // if we've already debugged a similar namespace metric and dimension key set. Purposely
      // not including the tag values.
      val ts = normalize(scrapeTimestamp, 60)
      val hashCode = hash(cacheEntry)
      val prevReport = outgoingLimiter.put(hashCode, scrapeTimestamp)

      val tags = Map(
        "aws.namespace" -> cacheEntry.getNamespace,
        "metrics"       -> cacheEntry.getMetric
      ).asJava

      scrapeState match {
        case ScrapeState.Empty =>
          registry.counter(scrapeEmpty.withTags(tags)).increment()
        case ScrapeState.Stale =>
          if (value.isDefined) {
            registry
              .distributionSummary(scrapePast.withTags(tags))
              .record((scrapeTimestamp - value.get.getTimestamp).toInt / 1000)
          }
        case ScrapeState.InTimeout =>
          if (value.isDefined) {
            registry
              .distributionSummary(scrapeWTimeout.withTags(tags))
              .record((scrapeTimestamp - value.get.getTimestamp).toInt / 1000)
          }
        case ScrapeState.OnTime =>
          registry.counter(scrapeOffset.withTags(tags)).increment()
        case ScrapeState.Future =>
          if (value.isDefined) {
            registry
              .distributionSummary(scrapeFuture.withTags(tags))
              .record((value.get.getTimestamp - scrapeTimestamp).toInt / 1000)
          }
      }

      if (prevReport == ts) return // here's our limiter

      val stream = new ByteArrayOutputStream()
      Using.resource(Json.newJsonGenerator(stream)) { json =>
        json.writeStartObject()
        json.writeStringField("state", "scrape")
        json.writeStringField("s", scrapeState.toString)
        json.writeNumberField("sts", scrapeTimestamp)
        json.writeStringField("ns", cacheEntry.getNamespace)
        json.writeStringField("metric", cacheEntry.getMetric)
        json.writeObjectFieldStart("tags")
        cacheEntry.getDimensionsList.asScala.foreach(d =>
          json.writeStringField(d.getName, d.getValue)
        )
        json.writeEndObject()

        if (value.isDefined) {
          val dp = value.get
          json.writeObjectFieldStart("publishValue")
          json.writeNumberField("ageScrape", (scrapeTimestamp - dp.getTimestamp).toInt / 1000)
          json.writeNumberField("ageOffset", (offsetTimestamp - dp.getTimestamp).toInt / 1000)
          json.writeNumberField("sum", dp.getSum)
          json.writeNumberField("min", dp.getMin)
          json.writeNumberField("max", dp.getMax)
          json.writeNumberField("count", dp.getCount)
          json.writeEndObject()
        }

        if (category.isDefined) {
          val cat = category.get
          json.writeObjectFieldStart("cat")
          json.writeNumberField("p", cat.period)
          json.writeNumberField("off", cat.endPeriodOffset)
          json.writeNumberField("to", cat.timeout.getOrElse(Duration.ofSeconds(0)).getSeconds)
          json.writeArrayFieldStart("tags")
          cat.dimensions.foreach(d => json.writeString(d))
          json.writeEndArray()
          json.writeEndObject()
        }

        if (cacheEntry.getDataCount > 0) {
          var step = 0L
          var last = 0L
          json.writeArrayFieldStart("cached")
          for (i <- 0 until cacheEntry.getDataCount) {
            val d = cacheEntry.getData(i)
            val dts = d.getTimestamp
            json.writeStartObject()
            json.writeNumberField("ts", dts)
            json.writeNumberField("ageScrape", (ts - dts).toInt / 1000)
            json.writeNumberField("sum", d.getSum())
            json.writeNumberField("min", d.getMin())
            json.writeNumberField("max", d.getMax)
            json.writeNumberField("count", d.getCount)
            if (last != 0) {
              step += (dts - last)
            }
            last = dts

            json.writeEndObject()
          }
          json.writeEndArray()

          if (cacheEntry.getDataCount >= 2) {
            step = ((step / (cacheEntry.getDataCount - 1)) / 1000).toInt
            json.writeNumberField("avgStep", step)
            registry.distributionSummary(scrapeStep.withTags(tags)).record(step)
          }
        }
        json.writeEndObject()
      }

      logger.info(stream.toString("UTF-8"))

    } catch {
      case ex: Exception => logger.error("whoops", ex)
    }
  }

  def hash(dp: FirehoseMetric, insertState: Option[InsertState]): Long = {
    var hash = XXHasher.hash(dp.namespace)
    hash = XXHasher.updateHash(hash, dp.metricName)
    dp.dimensions.sortBy(_.name()).foreach(d => hash = XXHasher.updateHash(hash, d.name()))
    insertState.map(state => hash = XXHasher.updateHash(hash, state.toString))
    hash
  }

  def hash(entry: CloudWatchCacheEntry): Long = {
    var hash = XXHasher.hash(entry.getNamespace)
    hash = XXHasher.updateHash(hash, entry.getMetric)
    entry.getDimensionsList.asScala
      .sortBy(_.getName)
      .foreach(d => hash = XXHasher.updateHash(hash, d.getName))
    hash
  }

  def hash(metric: Metric): Long = {
    var hash = XXHasher.hash(metric.namespace())
    hash = XXHasher.updateHash(hash, metric.metricName())
    metric
      .dimensions()
      .asScala
      .sortBy(_.name)
      .foreach(d => hash = XXHasher.updateHash(hash, d.name))
    hash
  }
}

object IncomingMatch extends Enumeration {

  type IncomingMatch = Value

  val DroppedNS, DroppedMetric, DroppedTag, DroppedFilter, DroppedOld, DroppedEmpty, Accepted =
    Value
}

object InsertState extends Enumeration {

  type InsertState = Value
  val New, OOO, Update, Append = Value
}

object ScrapeState extends Enumeration {

  type ScrapeState = Value

  val Empty, PurgedNS, PurgedMetric, PurgedTag, PurgedFilter, Stale, InTimeout, Future, OnTime =
    Value
}
