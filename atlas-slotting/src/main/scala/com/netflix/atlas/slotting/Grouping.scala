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

import java.nio.ByteBuffer
import java.time.Instant

import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import com.amazonaws.services.autoscaling.model.{Instance => AsgInstance}
import com.amazonaws.services.ec2.model.{Instance => Ec2Instance}
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Counter
import com.netflix.spectator.ipc.ServerGroup
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

case class AsgDetails(
  name: String,
  cluster: String,
  createdTime: Instant,
  desiredCapacity: Int,
  maxSize: Int,
  minSize: Int,
  instances: List[AsgInstanceDetails],
)

case class AsgInstanceDetails(
  instanceId: String,
  availabilityZone: String,
  lifecycleState: String,
)

case class Ec2InstanceDetails(
  privateIpAddress: String,
  publicIpAddress: Option[String],
  publicDnsName: Option[String],
  launchTime: Instant,
  imageId: String,
  instanceType: String,
)

case class SlottedAsgDetails(
  name: String,
  cluster: String,
  createdTime: Instant,
  desiredCapacity: Int,
  maxSize: Int,
  minSize: Int,
  instances: List[SlottedInstanceDetails],
) {
  require(
    instances.size <= desiredCapacity,
    s"instances.size (${instances.size}) > desiredCapacity ($desiredCapacity)"
  )
}

case class SlottedInstanceDetails(
  instanceId: String,
  privateIpAddress: String,
  publicIpAddress: Option[String],
  publicDnsName: Option[String],
  slot: Int,
  launchTime: Instant,
  imageId: String,
  instanceType: String,
  availabilityZone: String,
  lifecycleState: String,
)

trait Grouping extends StrictLogging {

  /** Parse the app name from an AutoScalingGroup name.
    *
    * See https://github.com/Netflix/spectator/pull/551 for details.
    *
    * @param name
    *     An AutoScalingGroup name.
    * @return
    *     An app name.
    */
  def getApp(name: String): String = {
    ServerGroup.parse(name).app
  }

  /** Parse the cluster name from an AutoScalingGroup name.
    *
    * See https://github.com/Netflix/spectator/pull/551 for details.
    *
    * @param name
    *     An AutoScalingGroup name.
    * @return
    *     A cluster name.
    */
  def getCluster(name: String): String = {
    ServerGroup.parse(name).cluster
  }

  /** Make a case class with details from an instance of an AutoScalingGroup.
    *
    * @param asg
    *     An AutoScalingGroup from a recent AWS API crawl.
    * @return
    *     A case class with selected fields representing the AutoScalingGroup.
    */
  def mkAsgDetails(asg: AutoScalingGroup): AsgDetails = {
    AsgDetails(
      asg.getAutoScalingGroupName,
      getCluster(asg.getAutoScalingGroupName),
      asg.getCreatedTime.toInstant,
      asg.getDesiredCapacity,
      asg.getMaxSize,
      asg.getMinSize,
      mkAsgInstanceDetailsList(asg.getInstances),
    )
  }

  /** Make a list of case classes with details from a list of AutoScalingGroup Model Instances.
    *
    * @param instances
    *     A list of AutoScalingGroup model Instances.
    * @return
    *     A list of case classes with selected fields representing the Instances.
    */
  def mkAsgInstanceDetailsList(instances: java.util.List[AsgInstance]): List[AsgInstanceDetails] = {
    instances.asScala.toList
      .map { i =>
        AsgInstanceDetails(
          i.getInstanceId,
          i.getAvailabilityZone,
          i.getLifecycleState,
        )
      }
  }

  /** Make a case class with details from an instance of an EC2 Model Instance.
    *
    * @param instance
    *     An EC2 model Instance from a recent AWS API crawl.
    * @return
    *     A case class with selected fields representing the Instance.
    */
  def mkEc2InstanceDetails(instance: Ec2Instance): Ec2InstanceDetails = {
    Ec2InstanceDetails(
      instance.getPrivateIpAddress,
      Option(instance.getPublicIpAddress),
      instance.getPublicDnsName match {
        case ""      => None
        case default => Some(default)
      },
      instance.getLaunchTime.toInstant,
      instance.getImageId,
      instance.getInstanceType,
    )
  }

  private val UNASSIGNED_SLOT: Int = -1

  /** Make a list of slotted instances for the group.
    *
    * The slots will be set to non-sentinel values during the merge step.
    *
    * @param asgDetails
    *     A case class describing an AutoScalingGroup from a recent AWS API crawl.
    * @param instanceInfo
    *     A map of instance information from a recent AWS API crawl.
    * @return
    *     A list of slotted instances for the group, with details combined from both data sources.
    */
  def mkSlottedInstanceDetailsList(
    asgDetails: AsgDetails,
    instanceInfo: Map[String, Ec2InstanceDetails]
  ): List[SlottedInstanceDetails] = {

    asgDetails.instances
      .filter { i =>
        instanceInfo.contains(i.instanceId)
      }
      .map { i =>
        val instance = instanceInfo(i.instanceId)

        SlottedInstanceDetails(
          i.instanceId,
          instance.privateIpAddress,
          instance.publicIpAddress,
          instance.publicDnsName,
          UNASSIGNED_SLOT,
          instance.launchTime,
          instance.imageId,
          instance.instanceType,
          i.availabilityZone,
          i.lifecycleState,
        )
      }
  }

  /** Assign a slot number to each instance in a new AutoScalingGroup, starting with 0.
    *
    * @param newAsgDetails
    *     An AutoScalingGroup from a recent AWS API crawl, without slot numbers.
    * @param instanceInfo
    *     A map of instance information from a recent AWS API crawl.
    * @return
    *     A sorted list of Instances with slot numbers, built from both AutoScaling and EC2 data.
    */
  def assignSlots(
    newAsgDetails: AsgDetails,
    instanceInfo: Map[String, Ec2InstanceDetails]
  ): List[SlottedInstanceDetails] = {
    mkSlottedInstanceDetailsList(newAsgDetails, instanceInfo).zipWithIndex
      .map {
        case (instance, slot) => instance.copy(slot = slot)
      }
  }

  /** Create a Gzip compressed JSON payload of ASG data with new slot numbers, for DynamoDB.
    *
    * Compression is necessary to ensure that the largest ASGs remain within the DynamoDB
    * item size limit of 400KB, including attribute names and values. For the typical case,
    * a 10:1 reduction in payload size has been observed with compression (1MB -> 94KB).
    *
    * The SlottedAsgDetails constructor can fail with an IllegalArgumentException when the
    * number of instances exceeds the desired capacity of the ASG. This is intended as a
    * double-check to keep the slot numbers stable for a statically-sized ASG.
    *
    * @param newAsgDetails
    *   An AutoScalingGroup from a recent AWS API crawl, without slot numbers.
    * @return
    *   A Gzip compressed JSON payload of ASG data with new slot numbers, for DynamoDB.
    */
  def mkNewDataAssignSlots(
    newAsgDetails: AsgDetails,
    instanceInfo: Map[String, Ec2InstanceDetails]
  ): ByteBuffer = {
    Util.compress(
      Json.encode(
        SlottedAsgDetails(
          newAsgDetails.name,
          newAsgDetails.cluster,
          newAsgDetails.createdTime,
          newAsgDetails.desiredCapacity,
          newAsgDetails.maxSize,
          newAsgDetails.minSize,
          assignSlots(newAsgDetails, instanceInfo),
        )
      )
    )
  }

  /** Make a map of existing slot assignments, which will be merged with new instances.
    *
    * @param asgDetails
    *   An AutoScalingGroup that was loaded from DynamoDB, with slot numbers.
    * @return
    *   A map of instanceIds to slot numbers.
    */
  def mkSlotMap(asgDetails: SlottedAsgDetails): Map[String, Int] = {
    asgDetails.instances
      .map(i => i.instanceId -> i.slot)
      .toMap
  }

  /** Merge slot numbers for each instance in a known AutoScalingGroup.
    *
    * If a slot number already exists for an instance, then reuse it. Otherwise, use the first
    * unassigned number, starting with 0.
    *
    * If the number of instances in the ASG exceeds the desired capacity for any reason, this
    * function will fail with a NoSuchElementException, rather than assign slots. This prevents
    * the slot map from being disturbed in the event of inconsistent results from AWS or a bug
    * in the Slotting Service code. The slot map update should be retried on the next iteration.
    *
    * @param oldAsgDetails
    *     An AutoScalingGroup loaded from DynamoDB, with slot numbers.
    * @param newAsgDetails
    *     An AutoScalingGroup from a recent AWS API crawl, without slot numbers.
    * @param instanceInfo
    *     A map of instance information from a recent AWS API crawl.
    * @return
    *     A sorted list of Instances with slot numbers, built from both AutoScaling and EC2 data.
    */
  def mergeSlots(
    oldAsgDetails: SlottedAsgDetails,
    newAsgDetails: AsgDetails,
    instanceInfo: Map[String, Ec2InstanceDetails]
  ): List[SlottedInstanceDetails] = {
    val newInstances = mkSlottedInstanceDetailsList(newAsgDetails, instanceInfo)

    // get the old slot map and remove instances that are no longer present
    val idSet = newInstances.map(_.instanceId).toSet
    val oldSlotMap = mkSlotMap(oldAsgDetails).filterKeys(idSet.contains)

    // create a new slot map, merging added instances into empty slots
    val addedInstances = newInstances.filterNot(i => oldSlotMap.contains(i.instanceId))
    val unusedSlots = (0 until newAsgDetails.desiredCapacity).toSet -- oldSlotMap.values
    val newSlotMap = oldSlotMap ++ addedInstances.zip(unusedSlots).map {
      case (i, slot) => i.instanceId -> slot
    }

    // update slots for the new instances and fail if the instances.size > desiredCapacity
    newInstances
      .map { i =>
        i.copy(slot = newSlotMap(i.instanceId))
      }
      .sortWith(_.slot < _.slot)
  }

  /** Create a Gzip compressed JSON payload of ASG data with merged slot numbers, for DynamoDB.
    *
    * Compression is necessary to ensure that the largest ASGs remain within the DynamoDB
    * item size limit of 400KB, including attribute names and values. For the typical case,
    * a 10:1 reduction in payload size has been observed with compression (1MB -> 94KB).
    *
    * The mergeSlots function can fail with a NoSuchElementException when the number of
    * instances exceeds the desired capacity of the ASG. This is intended as a double-check
    * to keep the slot numbers stable for a statically-sized ASG.
    *
    * The SlottedAsgDetails constructor can fail with an IllegalArgumentException when the
    * number of instances exceeds the desired capacity of the ASG. This is intended as a
    * double-check to keep the slot numbers stable for a statically-sized ASG.
    *
    * In both of these failure cases, return the old data. This will result in updating the
    * timestamp of the existing item in DynamoDB, while allowing the main processing tasks to
    * continue uninterrupted.
    *
    * @param oldData
    *   A Gzip compressed JSON payload of old ASG data with slot numbers, from DynamoDB.
    * @param newAsgDetails
    *   An AutoScalingGroup from a recent AWS API crawl, without slot numbers.
    * @return
    *   A Gzip compressed JSON payload of ASG data with merged slot numbers, for DynamoDB.
    */
  def mkNewDataMergeSlots(
    oldData: ByteBuffer,
    newAsgDetails: AsgDetails,
    instanceInfo: Map[String, Ec2InstanceDetails],
    slotsErrors: Counter
  ): ByteBuffer = {
    val oldAsgDetails = Json.decode[SlottedAsgDetails](Util.decompress(oldData))

    try {
      Util.compress(
        Json.encode(
          SlottedAsgDetails(
            newAsgDetails.name,
            newAsgDetails.cluster,
            newAsgDetails.createdTime,
            newAsgDetails.desiredCapacity,
            newAsgDetails.maxSize,
            newAsgDetails.minSize,
            mergeSlots(oldAsgDetails, newAsgDetails, instanceInfo),
          )
        )
      )
    } catch {
      case e: Exception =>
        logger.error(s"failed to merge slots ${newAsgDetails.name}: ${e.getMessage}")
        slotsErrors.increment()
        oldData
    }
  }
}
