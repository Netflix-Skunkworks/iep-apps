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
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.toTagMap
import com.netflix.atlas.cloudwatch.IncomingMatch.IncomingMatch
import com.netflix.atlas.cloudwatch.InsertState.InsertState
import com.netflix.atlas.cloudwatch.ScrapeState.ScrapeState
import com.netflix.atlas.json3.Json
import com.netflix.atlas.util.XXHasher
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.impl.Parser
import com.netflix.spectator.atlas.impl.QueryIndex
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Metric

import java.io.ByteArrayOutputStream
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
  private val configRules = config.getStringList("atlas.cloudwatch.debug.rules").asScala

  private val rules: QueryIndex[Boolean] = {
    val idx = QueryIndex.newInstance[Boolean](registry)
    configRules.foreach { rule =>
      val query = Parser.parseQuery(rule)
      idx.add(query, true)
    }
    idx
  }

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

  private def matches(tags: Map[String, String]): Boolean = {
    !rules.findMatches(k => tags.getOrElse(k, null)).isEmpty
  }

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
      !matches(toTagMap(dp)) &&
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
        case _ => // no-op
      }
      if (prevReport == ts) return // here's our limiter

      val stream = new ByteArrayOutputStream()
      Using.resource(Json.newJsonGenerator(stream)) { json =>
        json.writeStartObject()
        json.writeStringProperty("state", "incoming")
        json.writeNumberProperty("hash", dp.xxHash)
        json.writeNumberProperty("rts", timestamp)
        json.writeStringProperty("ns", dp.namespace)
        json.writeStringProperty("metric", dp.metricName)
        json.writeObjectPropertyStart("tags")
        dp.dimensions.foreach(d => json.writeStringProperty(d.name(), d.value()))
        json.writeEndObject()
        json.writeNumberProperty("dts", dp.datapoint.timestamp().toEpochMilli)
        json.writeNumberProperty(
          "ageFromNow",
          (timestamp - dp.datapoint.timestamp().toEpochMilli).toInt / 1000
        )
        json.writeNumberProperty("sum", dp.datapoint.sum())
        json.writeNumberProperty("min", dp.datapoint.minimum())
        json.writeNumberProperty("max", dp.datapoint.maximum())
        json.writeNumberProperty("count", dp.datapoint.sampleCount())
        json.writeStringProperty("unit", dp.datapoint.unit().toString)

        json.writeStringProperty("filtered", incomingMatch.toString)
        if (insertState.isDefined) {
          json.writeStringProperty("insert", insertState.get.toString)
        }

        if (category.isDefined) {
          val cat = category.get
          json.writeObjectPropertyStart("cat")
          json.writeNumberProperty("p", cat.period)
          json.writeNumberProperty("go", cat.graceOverride)
          json.writeArrayPropertyStart("tags")
          cat.dimensions.foreach(d => json.writeString(d))
          json.writeEndArray()
          json.writeEndObject()
        }

        if (!cacheEntries.isEmpty) {
          var step = 0L
          var last = 0L
          json.writeArrayPropertyStart("cached")
          for (i <- 0 until cacheEntries.size()) {
            val d = cacheEntries.get(i)
            val dts = d.getTimestamp
            json.writeStartObject()
            json.writeNumberProperty("ts", dts)
            json.writeNumberProperty("offFromWindow", (ts - dts).toInt / 1000)
            json.writeNumberProperty("sum", d.getSum())
            json.writeNumberProperty("min", d.getMin())
            json.writeNumberProperty("max", d.getMax)
            json.writeNumberProperty("count", d.getCount)
            if (last != 0) {
              step += (dts - last)
            }
            last = dts

            json.writeEndObject()
          }
          json.writeEndArray()

          if (cacheEntries.size() >= 2) {
            json.writeNumberProperty("avgStep", ((step / (cacheEntries.size() - 1)) / 1000).toInt)
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
    dataPoints: java.util.List[Datapoint] = Collections.emptyList(),
    dpAdded: Option[Datapoint] = None
  ): Unit = {
    if (config.isEmpty) return

    if (
      !matches(toTagMap(metric)) &&
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
        case _ => // no-op
      }
      if (prevReport == ts) return // here's our limiter

      val stream = new ByteArrayOutputStream()
      Using.resource(Json.newJsonGenerator(stream)) { json =>
        json.writeStartObject()
        json.writeStringProperty("state", "incoming")
        json.writeNumberProperty("hash", hashCode)
        json.writeNumberProperty("rts", timestamp)
        json.writeStringProperty("ns", metric.namespace())
        json.writeStringProperty("metric", metric.metricName())
        json.writeObjectPropertyStart("tags")
        metric.dimensions.asScala.foreach(d => json.writeStringProperty(d.name(), d.value()))
        json.writeEndObject()

        json.writeStringProperty("filtered", incomingMatch.toString)
        json.writeObjectPropertyStart("cat")
        json.writeNumberProperty("p", category.period)
        json.writeNumberProperty("go", category.graceOverride)
        json.writeArrayPropertyStart("tags")
        category.dimensions.foreach(d => json.writeString(d))
        json.writeEndArray()
        json.writeEndObject()
        dpAdded.map { dp =>
          json.writeNumberProperty("dpAdded", dp.timestamp().toEpochMilli)
        }

        if (!dataPoints.isEmpty) {
          var step = 0L
          var last = 0L
          json.writeArrayPropertyStart("polledData")
          for (i <- 0 until dataPoints.size()) {
            val d = dataPoints.get(i)
            val dts = d.timestamp().toEpochMilli
            json.writeStartObject()
            json.writeNumberProperty("ts", dts)
            json.writeNumberProperty("offFromWindow", (ts - dts).toInt / 1000)
            json.writeNumberProperty("sum", d.sum())
            json.writeNumberProperty("min", d.minimum())
            json.writeNumberProperty("max", d.maximum())
            json.writeNumberProperty("count", d.sampleCount())
            if (last != 0) {
              step += (dts - last)
            }
            last = dts

            json.writeEndObject()
          }
          json.writeEndArray()

          if (dataPoints.size() >= 2) {
            json.writeNumberProperty("avgStep", ((step / (dataPoints.size() - 1)) / 1000).toInt)
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

    if (!matches(toTagMap(cacheEntry))) return

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
        case _ => // no-op
      }

      if (prevReport == ts) return // here's our limiter

      val stream = new ByteArrayOutputStream()
      Using.resource(Json.newJsonGenerator(stream)) { json =>
        json.writeStartObject()
        json.writeStringProperty("state", "scrape")
        json.writeStringProperty("s", scrapeState.toString)
        json.writeNumberProperty("sts", scrapeTimestamp)
        json.writeStringProperty("ns", cacheEntry.getNamespace)
        json.writeStringProperty("metric", cacheEntry.getMetric)
        json.writeObjectPropertyStart("tags")
        cacheEntry.getDimensionsList.asScala.foreach(d =>
          json.writeStringProperty(d.getName, d.getValue)
        )
        json.writeEndObject()

        if (value.isDefined) {
          val dp = value.get
          json.writeObjectPropertyStart("publishValue")
          json.writeNumberProperty("ageScrape", (scrapeTimestamp - dp.getTimestamp).toInt / 1000)
          json.writeNumberProperty("ageOffset", (offsetTimestamp - dp.getTimestamp).toInt / 1000)
          json.writeNumberProperty("sum", dp.getSum)
          json.writeNumberProperty("min", dp.getMin)
          json.writeNumberProperty("max", dp.getMax)
          json.writeNumberProperty("count", dp.getCount)
          json.writeEndObject()
        }

        if (category.isDefined) {
          val cat = category.get
          json.writeObjectPropertyStart("cat")
          json.writeNumberProperty("p", cat.period)
          json.writeNumberProperty("go", cat.graceOverride)
          json.writeArrayPropertyStart("tags")
          cat.dimensions.foreach(d => json.writeString(d))
          json.writeEndArray()
          json.writeEndObject()
        }

        if (cacheEntry.getDataCount > 0) {
          var step = 0L
          var last = 0L
          json.writeArrayPropertyStart("cached")
          for (i <- 0 until cacheEntry.getDataCount) {
            val d = cacheEntry.getData(i)
            val dts = d.getTimestamp
            json.writeStartObject()
            json.writeNumberProperty("ts", dts)
            json.writeNumberProperty("ageScrape", (ts - dts).toInt / 1000)
            json.writeNumberProperty("sum", d.getSum())
            json.writeNumberProperty("min", d.getMin())
            json.writeNumberProperty("max", d.getMax)
            json.writeNumberProperty("count", d.getCount)
            if (last != 0) {
              step += (dts - last)
            }
            last = dts

            json.writeEndObject()
          }
          json.writeEndArray()

          if (cacheEntry.getDataCount >= 2) {
            step = ((step / (cacheEntry.getDataCount - 1)) / 1000).toInt
            json.writeNumberProperty("avgStep", step)
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

  val Stale, DroppedNS, DroppedMetric, DroppedTag, DroppedFilter, DroppedOld, DroppedEmpty,
    Accepted =
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
