package com.netflix.atlas.cloudwatch.poller

import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.atlas.AtlasConfig
import com.netflix.spectator.atlas.AtlasRegistry
import com.netflix.spectator.atlas.impl.EvaluatorConfig
import com.typesafe.config.Config

import java.time.Duration

class PublishClient(config: PublishConfig) extends AbstractService {

  private val publishRegistry = new AtlasRegistry(Clock.SYSTEM, config)

  def updateGauge(id: Id, value: Double): Unit = {
    publishRegistry.gauge(id).set(value)
  }

  def updateTimer(id: Id, value: Duration): Unit = {
    publishRegistry.timer(id).record(value)
  }

  def updateCounter(id: Id): Unit = {
    publishRegistry.counter(id).increment()
  }

  def updateDs(id: Id, value: Long): Unit = {
    publishRegistry.distributionSummary(id).record(value)
  }

  override def startImpl(): Unit = {
    publishRegistry.start()
  }

  override def stopImpl(): Unit = {
    publishRegistry.stop()
  }
}

class PublishConfig(config: Config, status: LeaderStatus, registry: Registry)
    extends AtlasConfig
    with EvaluatorConfig {

  private val maxMeters = super.maxNumberOfMeters()

  override def get(k: String): String = {
    val prop = s"atlas.cloudwatch.poller.publish.$k"
    if (config.hasPath(prop)) config.getString(prop) else null
  }

  override def enabled(): Boolean = {
    status.hasLeadership
  }

  override def lwcEnabled(): Boolean = {
    enabled()
  }

  override def debugRegistry(): Registry = registry

  /**
   * Value is cached because it is called in a hot-path if there are a lot of new meters
   * for the aggregator.
   */
  override def maxNumberOfMeters(): Int = maxMeters

  override def evaluatorStepSize(): Long = {
    lwcStep().toMillis
  }

  override def parallelMeasurementPolling(): Boolean = {
    true
  }

  override def delayGaugeAggregation(): Boolean = {
    false
  }
}
