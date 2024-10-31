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

import java.time.Duration
import java.util.concurrent.TimeUnit
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.DimensionFilter
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest
import software.amazon.awssdk.services.cloudwatch.model.RecentlyActive

import scala.concurrent.duration.DurationInt

/**
  * Category of metrics to fetch from CloudWatch. This will typically correspond with
  * a CloudWatch namespace, though there may be multiple categories per namespace if
  * there is some variation in the behavior. For example, some namespaces will use a
  * different period for some metrics.
  *
  * @param namespace
  *     CloudWatch namespace for the data.
  * @param period
  *     How frequently data in this category is updated. Atlas is meant for data that
  *     is continuously reported and requires a consistent step. To minimize confusion
  *     for CloudWatch data we use the last reported value in CloudWatch as long as it
  *     is within one period from the polling time. The period is also needed for
  *     performing rate conversions on some metrics.
  * @param graceOverride
  *     How many periods to look back for unpublished data. The default is defined in
  *     'atlas.cloudwatch.grace'
  * @param dimensions
  *     The dimensions to query for when getting data from CloudWatch. For the
  *     GetMetricData calls we have to explicitly specify all of the dimensions. In some
  *     cases CloudWatch has duplicate data for pre-computed aggregates. For example,
  *     ELB data is reported overall for the load balancer and by zone. For Atlas it
  *     is better to map in the most granular form and allow the aggregate to be done
  *     dynamically at query time.
  * @param account
  *     If defined, only provided list of accounts will be allowed for this category, 
  *     default all accounts are allowed.
  * @param metrics
  *     The set of metrics to fetch and metadata for how to convert them.
  * @param filter
  *     Query expression used to select the set of metrics which should get published.
  *     This can sometimes be useful for debugging or if there are many "spammy" metrics
  *     for a given category.
  * @param pollOffset
  *     An optional flag that tells the app to poll for data instead of expecting it from
  *     the Firehose stream. Represents an offset from midnight as to when to poll the
  *     data. E.g. S3 daily metrics may be available 7 hours after midnight so set an
  *     offset of 8 hours to make sure it is ready.
  */
case class MetricCategory(
  namespace: String,
  period: Int,
  graceOverride: Int,
  dimensions: List[String],
  accounts: Option[List[String]] = None,
  metrics: List[MetricDefinition],
  filter: Option[Query],
  pollOffset: Option[Duration] = None
) {

  /**
    * Whether or not the configuration has one or more monotonic counters.
    */
  val hasMonotonic = metrics.find(_.monotonicValue).isDefined

  /**
    * Returns a set of list requests to fetch the metadata for the metrics matching
    * this category. As there may be a lot of data in CloudWatch that we are not
    * interested in, the list is used to restrict to the subset we actually care
    * about rather than a single request fetching everything for the namespace.
    */
  def toListRequests: List[(MetricDefinition, ListMetricsRequest)] = {
    import scala.jdk.CollectionConverters.*
    metrics.map { m =>
      val reqBuilder = ListMetricsRequest
        .builder()
        .namespace(namespace)
        .metricName(m.name)
        .dimensions(dimensions.map(d => DimensionFilter.builder().name(d).build()).asJava)
      // Limit the listing to recent data for most metrics. CloudWatch only supports a
      // value of 3h for this setting. If the period is more than an hour, then do not
      // restrict it to recent data as it could be related to the data flow rather than
      // actually being inactivity.
      if (period < 3600)
        reqBuilder.recentlyActive(RecentlyActive.PT3_H)

      m -> reqBuilder.build()
    }
  }

  /**
    * Determines if the tags provided by Cloud Watch match those in the category config so
    * that unwanted aggregates or lower level data can be filtered out. Ignores any `nf.*`
    * tags.
    *
    * @param tags
    *     The non-null data point to evaluate.
    * @return
    *     True if the tags match those of the category config, false if there were fewer or
    *     more tags than expected.
    */
  def dimensionsMatch(tags: List[Dimension]): Boolean = {
    var matched = 0
    var extraTag = false
    tags.filter(d => !d.name().startsWith("nf.")).foreach { d =>
      if (dimensions.contains(d.name())) matched += 1 else extraTag = true
    }
    !extraTag && matched == dimensions.size
  }

  def accountMatch(tags: List[Dimension], allowList: List[String]): Boolean = {
    tags.exists(d => d.name().equals("nf.account") && allowList.contains(d.value()))
  }
}

object MetricCategory extends StrictLogging {

  private val interpreter = Interpreter(QueryVocabulary.allWords)

  private[cloudwatch] def parseQuery(query: String): Query = {
    interpreter.execute(query).stack match {
      case (q: Query) :: Nil => q
      case _ =>
        logger.warn(s"invalid query '$query', using default of ':true'")
        Query.True
    }
  }

  def fromConfig(config: Config): MetricCategory = {
    import scala.jdk.CollectionConverters.*
    val metrics = config.getConfigList("metrics").asScala.toList
    val filter =
      if (!config.hasPath("filter")) None
      else {
        Some(parseQuery(config.getString("filter")))
      }
    val period = if (config.hasPath("period")) {
      config.getDuration("period", TimeUnit.SECONDS).toInt
    } else {
      1.minute.toSeconds.toInt
    }
    val graceOverride =
      if (config.hasPath("grace-override")) config.getInt("grace-override") else -1
    val pollOffset =
      if (config.hasPath("poll-offset")) Some(config.getDuration("poll-offset")) else None
    val accounts =
      if (config.hasPath("accounts")) Some(config.getStringList("accounts").asScala.toList)
      else None

    apply(
      namespace = config.getString("namespace"),
      period = period,
      graceOverride = graceOverride,
      dimensions = config.getStringList("dimensions").asScala.toList,
      metrics = metrics.flatMap(MetricDefinition.fromConfig),
      filter = filter,
      accounts = accounts,
      pollOffset = pollOffset
    )
  }
}
