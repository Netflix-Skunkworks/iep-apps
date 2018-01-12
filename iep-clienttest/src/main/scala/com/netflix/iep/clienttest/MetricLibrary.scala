/*
 * Copyright 2014-2018 Netflix, Inc.
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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton

import com.netflix.servo.monitor.BasicDistributionSummary
import com.netflix.servo.monitor.BasicGauge
import com.netflix.servo.monitor.DoubleGauge
import com.netflix.servo.monitor.DynamicCounter
import com.netflix.servo.monitor.DynamicTimer
import com.netflix.servo.monitor.MonitorConfig
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter

trait MetricLibrary {
  def increment(name: String, tags: Map[String, String]): Unit

  def recordTime(name: String, tags: Map[String, String], amount: Long): Unit

  def recordAmount(name: String, tags: Map[String, String], amount: Long): Unit

  def set(name: String, tags: Map[String, String], v: Double): Unit

  def poll(name: String, tags: Map[String, String], f: => Double): Unit
}

@Singleton
class ServoMetricLibrary extends MetricLibrary {

  type JDouble = java.lang.Double

  private val distSummaries = new ConcurrentHashMap[MonitorConfig, BasicDistributionSummary]()
  private val gauges = new ConcurrentHashMap[MonitorConfig, DoubleGauge]()
  private val polledGauges = new ConcurrentHashMap[MonitorConfig, BasicGauge[JDouble]]()

  private def toMonitorConfig(name: String, tags: Map[String, String]): MonitorConfig = {
    tags
      .foldLeft(MonitorConfig.builder(name)) { (builder, t) =>
        builder.withTag(t._1, t._2)
      }
      .build()
  }

  override def increment(name: String, tags: Map[String, String]): Unit = {
    DynamicCounter.increment(toMonitorConfig(name, tags))
  }

  override def recordTime(name: String, tags: Map[String, String], amount: Long): Unit = {
    DynamicTimer.record(toMonitorConfig(name, tags), TimeUnit.SECONDS, amount, TimeUnit.NANOSECONDS)
  }

  override def recordAmount(name: String, tags: Map[String, String], amount: Long): Unit = {
    val config = toMonitorConfig(name, tags)
    distSummaries.computeIfAbsent(config, c => new BasicDistributionSummary(c)).record(amount)
  }

  override def set(name: String, tags: Map[String, String], v: Double): Unit = {
    val config = toMonitorConfig(name, tags)
    gauges.computeIfAbsent(config, c => new DoubleGauge(c)).set(v)
  }

  override def poll(name: String, tags: Map[String, String], f: => Double): Unit = {
    val config = toMonitorConfig(name, tags)
    polledGauges.computeIfAbsent(config, c => new BasicGauge[JDouble](c, () => f))
  }
}

@Singleton
class SpectatorMetricLibrary @Inject() (registry: Registry) extends MetricLibrary {

  private def toId(name: String, tags: Map[String, String]): Id = {
    tags.foldLeft(registry.createId(name)) { (id, t) =>
      id.withTag(t._1, t._2)
    }
  }

  override def increment(name: String, tags: Map[String, String]): Unit = {
    registry.counter(toId(name, tags)).increment()
  }

  override def recordTime(name: String, tags: Map[String, String], amount: Long): Unit = {
    registry.timer(toId(name, tags)).record(amount, TimeUnit.NANOSECONDS)
  }

  override def recordAmount(name: String, tags: Map[String, String], amount: Long): Unit = {
    registry.distributionSummary(toId(name, tags)).record(amount)
  }

  override def set(name: String, tags: Map[String, String], v: Double): Unit = {
    registry.gauge(toId(name, tags)).set(v)
  }

  override def poll(name: String, tags: Map[String, String], f: => Double): Unit = {
    PolledMeter.using(registry)
      .withId(toId(name, tags))
      .monitorValue(this, (obj: MetricLibrary) => f)
  }
}
