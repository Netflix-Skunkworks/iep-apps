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

import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import com.amazonaws.services.autoscaling.model.{Instance => AsgInstance}
import com.amazonaws.services.ec2.model.{Instance => Ec2Instance}
import com.netflix.frigga.Names
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

case class AsgDetails(
  cluster: String,
  createdTime: java.util.Date,
  desiredCapacity: Int,
  instances: List[AsgInstanceDetails],
  maxSize: Int,
  minSize: Int,
  name: String,
)

case class AsgInstanceDetails(
  availabilityZone: String,
  instanceId: String,
  lifecycleState: String,
)

case class Ec2InstanceDetails(
  imageId: String,
  instanceType: String,
  launchTime: java.util.Date,
  privateIpAddress: String,
  publicDnsName: String,
  publicIpAddress: Option[String],
)

case class SlottedAsgDetails(
  cluster: String,
  createdTime: java.util.Date,
  desiredCapacity: Int,
  instances: List[SlottedInstanceDetails],
  maxSize: Int,
  minSize: Int,
  name: String,
)

case class SlottedInstanceDetails(
  availabilityZone: String,
  imageId: String,
  instanceId: String,
  instanceType: String,
  launchTime: java.util.Date,
  lifecycleState: String,
  privateIpAddress: String,
  publicDnsName: String,
  publicIpAddress: Option[String],
  slot: Int,
)

trait Grouping extends StrictLogging {

  /** Instance state names that should be excluded during crawls.
    *
    * @param stateName
    *     An Ec2 Model Instance state name.
    * @return
    *     Is the state name excluded from crawl results?
    */
  def excludedInstanceState(stateName: String): Boolean = {
    Set(
      "pending",
      "shutting-down",
      "terminating",
      "terminated",
    ).contains(stateName)
  }

  /** Parse the app name from an AutoScalingGroup name.
    *
    * @param name
    *     An AutoScalingGroup name.
    * @return
    *     An app name.
    */
  def getApp(name: String): String = {
    Names.parseName(name).getApp
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
      getApp(asg.getAutoScalingGroupName),
      asg.getCreatedTime,
      asg.getDesiredCapacity,
      mkAsgInstanceDetailsList(asg.getInstances),
      asg.getMaxSize,
      asg.getMinSize,
      asg.getAutoScalingGroupName,
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
          i.getAvailabilityZone,
          i.getInstanceId,
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
      instance.getImageId,
      instance.getInstanceType,
      instance.getLaunchTime,
      instance.getPrivateIpAddress,
      instance.getPublicDnsName,
      Option(instance.getPublicIpAddress),
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
          i.availabilityZone,
          instance.imageId,
          i.instanceId,
          instance.instanceType,
          instance.launchTime,
          i.lifecycleState,
          instance.privateIpAddress,
          instance.publicDnsName,
          instance.publicIpAddress,
          UNASSIGNED_SLOT,
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
    val newInstances = mkSlottedInstanceDetailsList(newAsgDetails, instanceInfo)
    var unusedSlots: IndexedSeq[Int] = newInstances.indices

    newInstances
      .map { i =>
        val updatedSlot = unusedSlots.head
        unusedSlots = unusedSlots.tail
        i.copy(slot = updatedSlot)
      }
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
    val slotMap = mkSlotMap(oldAsgDetails)
    val newInstances = mkSlottedInstanceDetailsList(newAsgDetails, instanceInfo)

    val usedSlots: Set[Int] = newInstances
      .map(_.instanceId)
      .filter(slotMap.contains)
      .map(slotMap)
      .toSet

    var unusedSlots: IndexedSeq[Int] = newInstances.indices
      .filterNot(usedSlots.contains)

    newInstances
      .map { i =>
        val updatedSlot = slotMap.get(i.instanceId) match {
          case Some(slot) =>
            slot
          case None =>
            val slot = unusedSlots.head
            unusedSlots = unusedSlots.tail
            slot
        }

        i.copy(slot = updatedSlot)
      }
      .sortWith((a, b) => a.slot < b.slot)
  }
}
