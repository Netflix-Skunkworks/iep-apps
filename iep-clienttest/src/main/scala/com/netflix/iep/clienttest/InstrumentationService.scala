/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.iep.clienttest

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton

import com.netflix.iep.service.AbstractService
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

@Singleton
class InstrumentationService @Inject()(config: Config, metrics: MetricLibrary)
    extends AbstractService {

  private val logger = LoggerFactory.getLogger("")

  private val tagsPerMetric = config.getInt("netflix.iep.clienttest.tags-per-metric")

  private val numCounters = config.getInt("netflix.iep.clienttest.num-counters")
  private val numTimers = config.getInt("netflix.iep.clienttest.num-timers")
  private val numDistSummaries = config.getInt("netflix.iep.clienttest.num-dist-summaries")
  private val numGauges = config.getInt("netflix.iep.clienttest.num-gauges")
  private val numPolledGauges = config.getInt("netflix.iep.clienttest.num-polled-gauges")
  private val numSlowPolledGauges = config.getInt("netflix.iep.clienttest.num-slow-polled-gauges")

  // To minimize other noise in terms of memory use and computation we use the same base tag
  // set for all metrics.
  private val tagsData = (0 until tagsPerMetric).map { i =>
    val key = f"$i%05d"
    key -> UUID.randomUUID().toString
  }.toMap

  private val executor = Executors.newScheduledThreadPool(2)
  executor.scheduleWithFixedDelay(() => update(), 0L, 10, TimeUnit.SECONDS)

  // Polled sources only need to be registered once
  (0 until numPolledGauges).foreach { i =>
    metrics.poll("polledGauge", createTags(i), i.toDouble)
  }
  (0 until numSlowPolledGauges).foreach { i =>
    metrics.poll("slowPolledGauge", createTags(i), {
      Thread.sleep(120000)
      i.toDouble
    })
  }

  private def update(): Unit = {
    logger.info("update starting")
    logger.info(s"updating $numCounters counters")
    (0 until numCounters).foreach { i =>
      metrics.increment("counter", createTags(i))
    }
    logger.info(s"updating $numTimers timers")
    (0 until numTimers).foreach { i =>
      metrics.recordTime("timer", createTags(i), i)
    }
    logger.info(s"updating $numDistSummaries distribution summaries")
    (0 until numDistSummaries).foreach { i =>
      metrics.recordTime("distSummary", createTags(i), i)
    }
    logger.info(s"updating $numGauges gauges")
    (0 until numGauges).foreach { i =>
      metrics.set("gauge", createTags(i), i)
    }
    logger.info("update complete")
  }

  private def createTags(i: Int): Map[String, String] = {
    tagsData + ("id" -> i.toString)
  }

  override def startImpl(): Unit = ()

  override def stopImpl(): Unit = {
    executor.shutdownNow()
  }
}
