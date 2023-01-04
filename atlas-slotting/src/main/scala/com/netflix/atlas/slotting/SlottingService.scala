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
package com.netflix.atlas.slotting

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import com.netflix.atlas.json.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest
import software.amazon.awssdk.services.ec2.model.InstanceStateName

import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._
import scala.collection.immutable.SortedMap

class SlottingService(
  config: Config,
  registry: Registry,
  asgClient: AutoScalingClient,
  ddbClient: DynamoDbClient,
  ec2Client: Ec2Client,
  slottingCache: SlottingCache
) extends AbstractService
    with Grouping
    with DynamoOps
    with StrictLogging {

  type Item = java.util.Map[String, AttributeValue]

  private val clock = registry.clock()

  private val tableName = config.getString("aws.dynamodb.table-name")
  private val dynamodbErrors = registry.counter("dynamodb.errors")

  private val apps = config.getStringList("slotting.app-names").asScala.toSet

  @volatile
  private var asgs = Map.empty[String, AsgDetails]

  @volatile
  private var remainingAsgs = Set.empty[String]

  @volatile
  private var asgsAvailable = false

  private val asgPageSize = config.getInt("aws.autoscaling.page-size")

  private val crawlAsgsTimer = registry.timer("crawl.timer", "id", "asgs")
  private val crawlAsgsCount = registry.counter("crawl.count", "id", "asgs")
  private val crawlAsgsErrors = registry.counter("crawl.errors", "id", "asgs")

  private val lastUpdateAsgs = PolledMeter
    .using(registry)
    .withId(registry.createId("last.update", "id", "asgs"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  def crawlAsgsTask(): Unit = {
    if (!asgsAvailable) {
      val start = registry.clock().monotonicTime()
      var elapsed = 0L

      try {
        asgs = crawlAutoScalingGroups(asgPageSize, apps)
        remainingAsgs = asgs.keySet
        asgsAvailable = true
      } catch {
        case e: Exception =>
          crawlAsgsErrors.increment()
          throw e
      } finally {
        elapsed = registry.clock().monotonicTime() - start
        crawlAsgsTimer.record(elapsed, TimeUnit.NANOSECONDS)
      }

      logger.info(s"crawled ${asgs.size} asgs in ${fmtTime(elapsed)}")
      lastUpdateAsgs.set(clock.wallTime())
    }

    if (allDataAvailable) updateSlots()
  }

  /** Crawl ASGs and return a map of asgName -> AsgDetails, for defined appNames.
    *
    * Count the ASGs here, to avoid data consistency issues with publishing the count
    * on the minute boundary when the task runs.
    *
    */
  def crawlAutoScalingGroups(
    pageSize: Int,
    includedApps: Set[String]
  ): Map[String, AsgDetails] = {
    val request = DescribeAutoScalingGroupsRequest
      .builder()
      .maxRecords(pageSize)
      .build()

    asgClient
      .describeAutoScalingGroupsPaginator(request)
      .autoScalingGroups()
      .stream()
      .toScala(List)
      .filter(asg => includedApps.contains(getApp(asg.autoScalingGroupName())))
      .map { asg =>
        crawlAsgsCount.increment()
        asg.autoScalingGroupName() -> mkAsgDetails(asg)
      }
      .toMap
  }

  @volatile
  private var instanceInfo = Map.empty[String, Ec2InstanceDetails]

  @volatile
  private var instanceInfoAvailable = false

  private val ec2PageSize = config.getInt("aws.ec2.page-size")

  private val crawlInstancesTimer = registry.timer("crawl.timer", "id", "instances")
  private val crawlInstancesCount = registry.counter("crawl.count", "id", "instances")
  private val crawlInstancesErrors = registry.counter("crawl.errors", "id", "instances")

  private val lastUpdateInstances = PolledMeter
    .using(registry)
    .withId(registry.createId("last.update", "id", "instances"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  def crawlInstancesTask(): Unit = {
    if (!instanceInfoAvailable) {
      val start = registry.clock().monotonicTime()
      var elapsed = 0L

      try {
        instanceInfo = crawlInstances(ec2PageSize)
        instanceInfoAvailable = true
      } catch {
        case e: Exception =>
          crawlInstancesErrors.increment()
          throw e
      } finally {
        elapsed = registry.clock().monotonicTime() - start
        crawlInstancesTimer.record(elapsed, TimeUnit.NANOSECONDS)
      }

      logger.info(s"crawled ${instanceInfo.size} instances in ${fmtTime(elapsed)}")
      lastUpdateInstances.set(clock.wallTime())
    }

    if (allDataAvailable) updateSlots()
  }

  /** Crawl Ec2 Instances and return a map of instanceId -> Ec2InstanceDetails.
    *
    * Filtering out instances in states other than "running" keeps the slot numbers stable for
    * a given desired capacity.
    *
    * See [[software.amazon.awssdk.services.ec2.model.InstanceStateName]] for a list of possible states.
    *
    * Count the instances here, to avoid data consistency issues with publishing the count
    * on the minute boundary when the task runs.
    *
    */
  def crawlInstances(pageSize: Int): Map[String, Ec2InstanceDetails] = {
    val request = DescribeInstancesRequest
      .builder()
      .maxResults(pageSize)
      .build()

    ec2Client
      .describeInstancesPaginator(request)
      .reservations()
      .stream()
      .flatMap(_.instances().stream())
      .filter(_.state().name() == InstanceStateName.RUNNING)
      .map { instance =>
        crawlInstancesCount.increment()
        instance.instanceId() -> mkEc2InstanceDetails(instance)
      }
      .toScala(List)
      .toMap
  }

  private val lastUpdateCache = PolledMeter
    .using(registry)
    .withId(registry.createId("last.update", "id", "cache"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  private def updateCacheTask(): Unit = {
    val request = activeItemsScanRequest(tableName)
    val updatedAsgs = SortedMap.empty[String, SlottedAsgDetails] ++ ddbClient
      .scanPaginator(request)
      .items()
      .stream()
      .map { item =>
        val name = item.get(Name).s()
        val data =
          Json.decode[SlottedAsgDetails](Util.decompress(item.get(Data).b().asByteBuffer()))
        name -> data
      }
      .toScala(List)
      .toMap

    logger.info(s"replace cache with ${updatedAsgs.size} active asgs")
    slottingCache.asgs = updatedAsgs
    lastUpdateCache.set(clock.wallTime())
  }

  private val deletedCount = registry.counter("deleted.count")

  private val lastUpdateJanitor = PolledMeter
    .using(registry)
    .withId(registry.createId("last.update", "id", "janitor"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  private val cutoffInterval = config.getDuration("slotting.cutoff-interval")

  def janitorTask(): Unit = {
    var count = 0
    val request = oldItemsScanRequest(tableName, cutoffInterval)
    ddbClient
      .scanPaginator(request)
      .items()
      .stream()
      .forEach { item =>
        val name = item.get(Name).s()
        try {
          logger.debug(s"delete item $name")
          ddbClient.deleteItem(deleteItemRequest(tableName, name))
          count += 1
        } catch {
          case e: Exception =>
            logger.error(s"failed to delete item $name: ${e.getMessage}")
            dynamodbErrors.increment()
        }
      }

    logger.info(s"removed $count items older than $cutoffInterval from table $tableName")
    deletedCount.increment(count)
    lastUpdateJanitor.set(clock.wallTime())
  }

  override def startImpl(): Unit = {
    val desiredRead = Util.getLongOrDefault(config, "aws.dynamodb.read-capacity")
    val desiredWrite = Util.getLongOrDefault(config, "aws.dynamodb.write-capacity")

    initTable(ddbClient, tableName, desiredRead, desiredWrite, dynamodbErrors)

    updateCacheTask()

    val asgInterval = config.getDuration("aws.autoscaling.crawl-interval")
    val ec2Interval = config.getDuration("aws.ec2.crawl-interval")
    val cacheInterval = config.getDuration("slotting.cache-load-interval")
    val janitorInterval = config.getDuration("slotting.janitor-interval")

    Util.startScheduler(registry, "crawlAsgs", asgInterval, () => crawlAsgsTask())
    Util.startScheduler(registry, "crawlInstances", ec2Interval, () => crawlInstancesTask())
    Util.startScheduler(registry, "cacheLoad", cacheInterval, () => updateCacheTask())
    Util.startScheduler(registry, "janitor", janitorInterval, () => janitorTask())
  }

  override def stopImpl(): Unit = {}

  def allDataAvailable: Boolean = {
    asgsAvailable && instanceInfoAvailable
  }

  def fmtTime(elapsed: Long): String = {
    f"${elapsed / 1000000000d}%.2f seconds"
  }

  private val lastUpdateSlots = PolledMeter
    .using(registry)
    .withId(registry.createId("last.update", "id", "slots"))
    .monitorValue(
      new AtomicLong(clock.wallTime),
      Functions.AGE
    )

  def updateSlots(): Unit = {
    val request = ScanRequest.builder().tableName(tableName).build()
    ddbClient
      .scan(request)
      .items()
      .stream()
      .forEach { item =>
        val name = item.get(Name).s()
        val active = item.get(Active).bool()

        if (remainingAsgs.contains(name))
          updateItem(name, item, asgs(name))
        else if (active)
          deactivateItem(name, item)
      }

    remainingAsgs.foreach { name =>
      addItem(name, asgs(name))
    }

    lastUpdateSlots.set(clock.wallTime)

    asgsAvailable = false
    instanceInfoAvailable = false
  }

  private val slotsChangedId = registry.createId("slots.changed")
  private val slotsErrorsId = registry.createId("slots.errors")

  def updateItem(
    name: String,
    item: Item,
    newAsgDetails: AsgDetails
  ): Unit = {
    val oldData = item.get(Data).b().asByteBuffer()
    val slotsErrors = registry.counter(slotsErrorsId.withTag("asg", name))
    val newData = mkNewDataMergeSlots(oldData, newAsgDetails, instanceInfo, slotsErrors)

    try {
      if (newData == oldData) {
        logger.debug(s"update timestamp for asg $name")
        val timestamp = item.get(Timestamp).n().toLong
        ddbClient.updateItem(updateTimestampItemRequest(tableName, name, timestamp))
      } else {
        logger.info(s"merge slots for asg $name")
        ddbClient.updateItem(updateAsgItemRequest(tableName, name, oldData, newData))
        registry.counter(slotsChangedId.withTag("asg", name)).increment()
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to update item $name: ${e.getMessage}")
        dynamodbErrors.increment()
    }

    remainingAsgs = remainingAsgs - name
  }

  def deactivateItem(name: String, item: Item): Unit = {
    try {
      logger.info(s"deactivate asg $name")
      ddbClient.updateItem(deactivateAsgItemRequest(tableName, name))
    } catch {
      case e: Exception =>
        logger.error(s"failed to update item $name: ${e.getMessage}")
        dynamodbErrors.increment()
    }
  }

  def addItem(name: String, newAsgDetails: AsgDetails): Unit = {
    try {
      val newData = mkNewDataAssignSlots(newAsgDetails, instanceInfo)
      logger.info(s"assign slots for asg $name")
      ddbClient.putItem(putAsgItemRequest(tableName, name, newData))
      registry.counter(slotsChangedId.withTag("asg", name)).increment()
    } catch {
      case e: IllegalArgumentException =>
        logger.error(s"failed to assign slots, not updating item $name: ${e.getMessage}")
        registry.counter(slotsErrorsId.withTag("asg", name)).increment()
      case e: Exception =>
        logger.error(s"failed to update item $name: ${e.getMessage}")
        dynamodbErrors.increment()
    }
  }
}
