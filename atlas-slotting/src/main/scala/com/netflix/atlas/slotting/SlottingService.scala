/*
 * Copyright 2014-2019 Netflix, Inc.
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

import com.amazonaws.services.autoscaling.AmazonAutoScaling
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.netflix.atlas.json.Json
import com.netflix.iep.aws.Pagination
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.inject.Singleton

import scala.collection.JavaConverters._

@Singleton
class SlottingService @Inject()(
  config: Config,
  registry: Registry,
  asgClient: AmazonAutoScaling,
  ddbClient: AmazonDynamoDB,
  ec2Client: AmazonEC2,
  slottingCache: SlottingCache
) extends AbstractService
    with AwsTypes
    with Grouping
    with DynamoOps
    with StrictLogging {

  private val clock = registry.clock()

  private val tableName = config.getString("dynamoDb.tableName")
  private val dynamoDb = new DynamoDB(ddbClient)
  private val dynamoDbErrors = registry.counter("dynamoDb.errors")

  private val apps = config.getStringList("slotting.appNames").asScala.toSet

  @volatile
  private var asgs = Map.empty[String, AsgDetails]

  @volatile
  private var remainingAsgs = Set.empty[String]

  @volatile
  private var asgsAvailable = false

  private val asgPageSize = config.getInt("aws.autoScaling.pageSize")

  private val crawlAsgsTimer = registry.timer("crawl.timer", "id", "asgs")
  private val crawlAsgsCount = registry.counter("crawl.count", "id", "asgs")
  private val crawlAsgsErrors = registry.counter("crawl.errors", "id", "asgs")

  private val lastUpdateAsgs = PolledMeter
    .using(registry)
    .withId(registry.createId("lastUpdate", "id", "asgs"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  private val crawlAsgsThread = Util.createBackgroundThread(
    "crawlAsgsThread",
    config.getDuration("aws.autoScaling.crawlInterval"),
    () => {
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
        crawlAsgsCount.increment(asgs.size)
        lastUpdateAsgs.set(clock.wallTime())
      }

      if (allDataAvailable) updateSlots()
    }
  )

  def crawlAutoScalingGroups(
    pageSize: Int,
    includedApps: Set[String]
  ): Map[String, AsgDetails] = {
    val request: DescribeAutoScalingGroupsRequest =
      new DescribeAutoScalingGroupsRequest()
        .withMaxRecords(pageSize)

    Pagination
      .createIterator[AsgReq, AsgRes](request, asgClient.describeAutoScalingGroups)
      .asScala
      .flatMap(_.getAutoScalingGroups.asScala)
      .filter(asg => includedApps.contains(getApp(asg.getAutoScalingGroupName)))
      .map(asg => asg.getAutoScalingGroupName -> mkAsgDetails(asg))
      .toMap
  }

  @volatile
  private var instanceInfo = Map.empty[String, Ec2InstanceDetails]

  @volatile
  private var instanceInfoAvailable = false

  private val ec2PageSize = config.getInt("aws.ec2.pageSize")

  private val crawlInstancesTimer = registry.timer("crawl.timer", "id", "instances")
  private val crawlInstancesCount = registry.counter("crawl.count", "id", "instances")
  private val crawlInstancesErrors = registry.counter("crawl.errors", "id", "instances")

  private val lastUpdateInstances = PolledMeter
    .using(registry)
    .withId(registry.createId("lastUpdate", "id", "instances"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  private val crawlInstancesThread = Util.createBackgroundThread(
    "crawlInstancesThread",
    config.getDuration("aws.ec2.crawlInterval"),
    () => {
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
        crawlInstancesCount.increment(instanceInfo.size)
        lastUpdateInstances.set(clock.wallTime())
      }

      if (allDataAvailable) updateSlots()
    }
  )

  def crawlInstances(pageSize: Int): Map[String, Ec2InstanceDetails] = {
    val request: DescribeInstancesRequest =
      new DescribeInstancesRequest()
        .withMaxResults(pageSize)

    Pagination
      .createIterator[Ec2Req, Ec2Res](request, ec2Client.describeInstances)
      .asScala
      .flatMap(_.getReservations.asScala)
      .flatMap(_.getInstances.asScala)
      .filterNot(i => excludedInstanceState(i.getState.getName))
      .map(i => i.getInstanceId -> mkEc2InstanceDetails(i))
      .toMap
  }

  private val lastUpdateCache = PolledMeter
    .using(registry)
    .withId(registry.createId("lastUpdate", "id", "cache"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  private val cacheLoadThread = Util.createBackgroundThread(
    "cacheLoadThread",
    config.getDuration("slotting.cacheLoadInterval"),
    () => updateCache()
  )

  private def updateCache(): Unit = {
    var updatedAsgs = Map.empty[String, SlottedAsgDetails]

    val table = dynamoDb.getTable(tableName)
    val iter = table.scan(scanActiveItems()).iterator

    while (iter.hasNext) {
      val item = iter.next
      val name = item.getString(Name)
      val data = Json.decode[SlottedAsgDetails](decompress(item.getByteBuffer(Data)))
      updatedAsgs += (name -> data)
    }

    logger.info(s"replace cache with ${updatedAsgs.size} active asgs")
    slottingCache.asgs = updatedAsgs
    lastUpdateCache.set(clock.wallTime())
  }

  private val lastUpdateJanitor = PolledMeter
    .using(registry)
    .withId(registry.createId("lastUpdate", "id", "janitor"))
    .monitorValue(new AtomicLong(clock.wallTime()), Functions.AGE)

  private val cutoffInterval = config.getDuration("slotting.cutoffInterval")

  private val janitorThread = Util.createBackgroundThread(
    "janitorThread",
    config.getDuration("slotting.janitorInterval"),
    () => {
      val table = dynamoDb.getTable(tableName)
      val iter = table.scan(scanOldItems(cutoffInterval)).iterator
      var count = 0

      while (iter.hasNext) {
        val item = iter.next
        val name = item.getString(Name)

        try {
          logger.debug(s"delete item $name")
          count += 1
          table.deleteItem(Name, name)
        } catch {
          case e: Exception =>
            logger.error(s"failed to delete item $name: ${e.getMessage}")
            dynamoDbErrors.increment()
        }
      }

      logger.info(s"removed $count items older than $cutoffInterval from table $tableName")
      lastUpdateJanitor.set(clock.wallTime())
    }
  )

  override def startImpl(): Unit = {
    val desiredRead = config.getLong("dynamoDb.readCapacity")
    val desiredWrite = config.getLong("dynamoDb.writeCapacity")

    initTable(logger, ddbClient, tableName, desiredRead, desiredWrite, dynamoDbErrors)

    updateCache()

    crawlAsgsThread.setDaemon(true)
    crawlAsgsThread.start()

    crawlInstancesThread.setDaemon(true)
    crawlInstancesThread.start()

    cacheLoadThread.setDaemon(true)
    cacheLoadThread.start()

    janitorThread.setDaemon(true)
    janitorThread.start()
  }

  override def stopImpl(): Unit = {
    ddbClient match {
      case c: AmazonDynamoDBClient => c.shutdown()
      case _                       =>
    }

    ec2Client match {
      case c: AmazonEC2Client => c.shutdown()
      case _                  =>
    }

    asgClient match {
      case c: AmazonAutoScalingClient => c.shutdown()
      case _                          =>
    }
  }

  def allDataAvailable: Boolean = {
    asgsAvailable && instanceInfoAvailable
  }

  def fmtTime(elapsed: Long): String = {
    f"${elapsed / 1000000000D}%.2f seconds"
  }

  private val lastUpdateSlots = PolledMeter
    .using(registry)
    .withId(registry.createId("lastUpdate", "id", "slots"))
    .monitorValue(
      new AtomicLong(clock.wallTime),
      Functions.AGE
    )

  def updateSlots(): Unit = {
    val table = dynamoDb.getTable(tableName)
    val iter = table.scan().iterator

    while (iter.hasNext) {
      val item = iter.next
      val name = item.getString(Name)
      val active = item.getBOOL(Active)

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

  private val slotsChange = registry.createId("slots.change")

  def updateItem(
    name: String,
    item: Item,
    newAsgDetails: AsgDetails
  ): Unit = {
    val oldData = item.getByteBuffer("data")
    val oldAsgDetails = Json.decode[SlottedAsgDetails](decompress(oldData))

    val newData = compress(
      Json.encode(
        SlottedAsgDetails(
          newAsgDetails.cluster,
          newAsgDetails.createdTime,
          newAsgDetails.desiredCapacity,
          mergeSlots(oldAsgDetails, newAsgDetails, instanceInfo),
          newAsgDetails.maxSize,
          newAsgDetails.minSize,
          newAsgDetails.name,
        )
      )
    )

    val table = dynamoDb.getTable(tableName)

    try {
      if (newData == oldData) {
        logger.debug(s"update timestamp for asg $name")
        table.updateItem(updateTimestamp(name, oldData))
      } else {
        logger.info(s"merge slots for asg $name")
        table.updateItem(updateSlottedAsg(name, oldData, newData))
        registry.counter(slotsChange.withTag("asg", name)).increment()
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to update item $name: ${e.getMessage}")
        dynamoDbErrors.increment()
    }

    remainingAsgs = remainingAsgs - name
  }

  def deactivateItem(name: String, item: Item): Unit = {
    val oldData = item.getByteBuffer("data")

    val table = dynamoDb.getTable(tableName)

    try {
      logger.info(s"deactivate asg $name")
      table.updateItem(deactivateAsg(name, oldData))
    } catch {
      case e: Exception =>
        logger.error(s"failed to update item $name: ${e.getMessage}")
        dynamoDbErrors.increment()
    }
  }

  def addItem(name: String, newAsgDetails: AsgDetails): Unit = {
    val newData = compress(
      Json.encode(
        SlottedAsgDetails(
          newAsgDetails.cluster,
          newAsgDetails.createdTime,
          newAsgDetails.desiredCapacity,
          assignSlots(newAsgDetails, instanceInfo),
          newAsgDetails.maxSize,
          newAsgDetails.minSize,
          newAsgDetails.name,
        )
      )
    )

    val table = dynamoDb.getTable(tableName)

    try {
      logger.info(s"assign slots for asg $name")
      table.putItem(putSlottedAsg(name, newData))
      registry.counter(slotsChange.withTag("asg", name)).increment()
    } catch {
      case e: Exception =>
        logger.error(s"failed to update item $name: ${e.getMessage}")
        dynamoDbErrors.increment()
    }
  }
}
