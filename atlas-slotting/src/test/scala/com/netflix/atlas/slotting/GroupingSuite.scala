/*
 * Copyright 2014-2025 Netflix, Inc.
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

import java.time.Instant
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.DefaultRegistry
import munit.FunSuite
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup
import software.amazon.awssdk.services.autoscaling.model.SuspendedProcess
import software.amazon.awssdk.services.autoscaling.model.Instance as AsgInstance
import software.amazon.awssdk.services.ec2.model.Instance as Ec2Instance

import scala.jdk.CollectionConverters.*
import scala.io.Source
import scala.util.Using

class GroupingSuite extends FunSuite with Grouping {

  test("get app") {
    val res = List(
      "atlas_app-v001",
      "atlas_app-main-v002",
      "atlas_app-main-all-v003"
    ).map(getApp)

    assertEquals(res, List("atlas_app", "atlas_app", "atlas_app"))
  }

  test("get cluster") {
    val res = List(
      "atlas_app-v001",
      "atlas_app-main-v002",
      "atlas_app-main-all-v003"
    ).map(getCluster)

    assertEquals(res, List("atlas_app", "atlas_app-main", "atlas_app-main-all"))
  }

  test("make asg details") {
    val asgInstances = List(
      AsgInstance
        .builder()
        .availabilityZone("us-west-2b")
        .lifecycleState("InService")
        .instanceId("i-001")
        .build(),
      AsgInstance
        .builder()
        .availabilityZone("us-west-2a")
        .lifecycleState("InService")
        .instanceId("i-002")
        .build(),
      AsgInstance
        .builder()
        .availabilityZone("us-west-2b")
        .lifecycleState("InService")
        .instanceId("i-003")
        .build()
    ).asJava

    val asg = AutoScalingGroup
      .builder()
      .autoScalingGroupName("atlas_app-main-all-v001")
      .createdTime(Instant.now())
      .desiredCapacity(3)
      .instances(asgInstances)
      .maxSize(6)
      .minSize(0)
      .build()

    val asgDetails = mkAsgDetails(asg)

    assertEquals(asgDetails.name, "atlas_app-main-all-v001")
    assertEquals(asgDetails.cluster, "atlas_app-main-all")
    assertEquals(asgDetails.desiredCapacity, 3)
    assertEquals(asgDetails.maxSize, 6)
    assertEquals(asgDetails.minSize, 0)
    assertEquals(asgDetails.instances.map(_.instanceId), List("i-001", "i-002", "i-003"))
  }

  test("make isDisabled flag") {
    val asgNoSuspended = AutoScalingGroup
      .builder()
      .build()

    val asgLaunchSuspended = AutoScalingGroup
      .builder()
      .suspendedProcesses(
        SuspendedProcess.builder().processName("Launch").build()
      )
      .build()

    assertEquals(mkIsDisabled(asgNoSuspended), false)
    assertEquals(mkIsDisabled(asgLaunchSuspended), true)
  }

  test("make ec2 instance details") {
    val instance = Ec2Instance
      .builder()
      .imageId("ami-001")
      .instanceType("r4.large")
      .launchTime(Instant.now())
      .privateIpAddress("192.168.1.1")
      .publicDnsName("")
      .build()

    val details = mkEc2InstanceDetails(instance)

    assertEquals(details.imageId, "ami-001")
    assertEquals(details.instanceType, "r4.large")
    assertEquals(details.privateIpAddress.get, "192.168.1.1")
  }

  test("make slotted instance details") {
    val asgInstances = newASGDetails()

    val asgDetails = mkASGDetails(asgInstances)

    val instanceInfo = Map(
      "i-001" -> mkEc2(1),
      "i-002" -> mkEc2(2),
      "i-003" -> mkEc2(3)
    )

    val slotted = mkSlottedInstanceDetailsList(asgDetails, instanceInfo)

    assertEquals(slotted.map(_.instanceId), List("i-001", "i-002", "i-003"))
    assertEquals(
      slotted.map(_.privateIpAddress.get),
      List("192.168.1.1", "192.168.1.2", "192.168.1.3")
    )
    assertEquals(slotted.map(_.slot), List(-1, -1, -1))
  }

  test("slotted asg details - too many instances") {
    val instances = loadASGDetails

    val caught = intercept[IllegalArgumentException] {
      SlottedAsgDetails(
        "atlas_app-main-all-v001",
        "atlas_app-main-all",
        Instant.now(),
        2,
        4,
        0,
        isDisabled = false,
        instances
      )
    }

    assertEquals(caught.getMessage, "requirement failed: instances.size (3) > desiredCapacity (2)")
  }

  test("assign slots") {
    val asgInstances = newASGDetails()

    val asgDetails = mkASGDetails(asgInstances)

    val instanceInfo = Map(
      "i-001" -> mkEc2(1),
      "i-002" -> mkEc2(2),
      "i-003" -> mkEc2(3)
    )

    val slotted = assignSlots(asgDetails, instanceInfo)

    assertEquals(slotted.map(_.instanceId), List("i-001", "i-002", "i-003"))
    assertEquals(
      slotted.map(_.privateIpAddress.get),
      List("192.168.1.1", "192.168.1.2", "192.168.1.3")
    )
    assertEquals(slotted.map(_.slot), List(0, 1, 2))
  }

  private def loadSlottedInstanceDetails(resource: String): SlottedInstanceDetails = {
    Using.resource(Source.fromURL(getClass.getResource(resource))) { src =>
      Json.decode[SlottedInstanceDetails](src.mkString)
    }
  }

  test("mk new data merge slots - empty to partially up") {
    val oldAsgInstances = List.empty[SlottedInstanceDetails]

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()
      .filterNot(_.instanceId == "i-002")

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> toEc2(loadASGDetails(0)),
      "i-003" -> toEc2(loadASGDetails(2))
    )

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(
      newAsgDetails,
      List(
        loadASGDetails(0),
        toSlottedInstance("i-003", instanceInfo, "us-west-2b", "InService", 1)
      )
    )
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("mk new data merge slots - empty to one remaining still starting") {
    val oldAsgInstances = List.empty[SlottedInstanceDetails]

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()
      .filterNot(_.instanceId == "i-002")
      .appended(AsgInstanceDetails("i-002", "us-west-2a", "Starting"))

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> toEc2(loadASGDetails(0)),
      "i-002" -> toEc2(loadASGDetails(1)),
      "i-003" -> toEc2(loadASGDetails(2))
    )

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(
      newAsgDetails,
      List(
        loadASGDetails(0),
        toSlottedInstance("i-003", instanceInfo, "us-west-2b", "InService", 1),
        toSlottedInstance("i-002", instanceInfo, "us-west-2a", "Starting", 2)
      )
    )
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("mk new data merge slots - single replacement") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()
      .filterNot(_.instanceId == "i-002")
      .appended(AsgInstanceDetails("i-004", "us-west-2a", "InService"))

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> toEc2(oldAsgInstances(0)),
      "i-003" -> toEc2(oldAsgInstances(2)),
      "i-004" -> mkEc2(4)
    )

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(
      newAsgDetails,
      List(
        oldAsgInstances(0),
        toSlottedInstance("i-004", instanceInfo, "us-west-2a", "InService", 1),
        oldAsgInstances(2)
      )
    )
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("merge slots - too many instances") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()
      .filterNot(_.instanceId == "i-002")
      .appended(AsgInstanceDetails("i-004", "us-west-2a", "InService"))
      .appended(AsgInstanceDetails("i-005", "us-west-2a", "InService"))

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> mkEc2(1),
      "i-003" -> mkEc2(3),
      "i-004" -> mkEc2(4),
      "i-005" -> mkEc2(5)
    )

    val caught = intercept[NoSuchElementException] {
      mergeSlots(oldAsgSlotted, newAsgDetails, instanceInfo)
    }

    assertEquals(caught.getMessage, "key not found: i-005")
  }

  test("mk new data merge slots - too many instances") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()
      .filterNot(_.instanceId == "i-002")
      .appended(AsgInstanceDetails("i-004", "us-west-2a", "InService"))
      .appended(AsgInstanceDetails("i-005", "us-west-2a", "InService"))

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> mkEc2(1),
      "i-003" -> mkEc2(3),
      "i-004" -> mkEc2(4),
      "i-005" -> mkEc2(5)
    )

    runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo, true)
  }

  test("mk new data merge slots - too many instances, no entry in instanceInfo") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()
      .filterNot(_.instanceId == "i-002")
      .appended(AsgInstanceDetails("i-004", "us-west-2a", "InService"))
      .appended(AsgInstanceDetails("i-005", "us-west-2a", "Terminating"))

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> toEc2(oldAsgInstances(0)),
      "i-003" -> toEc2(oldAsgInstances(2)),
      "i-004" -> mkEc2(4)
    )

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(
      newAsgDetails,
      List(
        oldAsgInstances(0),
        toSlottedInstance("i-004", instanceInfo, "us-west-2a", "InService", 1),
        oldAsgInstances(2)
      )
    )
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("mk new data merge slots - to all down") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = List(
      AsgInstanceDetails("i-001", "us-west-2b", "Down"),
      AsgInstanceDetails("i-002", "us-west-2b", "Down"),
      AsgInstanceDetails("i-003", "us-west-2b", "Down")
    )

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> toEc2(oldAsgInstances(0)),
      "i-002" -> toEc2(oldAsgInstances(1)),
      "i-003" -> toEc2(oldAsgInstances(2))
    )

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(
      newAsgDetails,
      List(
        loadASGDetails(0).copy(lifecycleState = "Down"),
        loadASGDetails(1).copy(lifecycleState = "Down"),
        loadASGDetails(2).copy(lifecycleState = "Down")
      )
    )
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("mk new data merge slots - full to empty") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = List.empty[AsgInstanceDetails]

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map(
      "i-001" -> toEc2(oldAsgInstances(0)),
      "i-002" -> toEc2(oldAsgInstances(1)),
      "i-003" -> toEc2(oldAsgInstances(2))
    )

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(newAsgDetails, List.empty[SlottedInstanceDetails])
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("mk new data merge slots - instance info empty") {
    val oldAsgInstances = loadASGDetails

    val oldAsgSlotted = mkSlottedASGDetails(oldAsgInstances)

    val newAsgInstances = newASGDetails()

    val newAsgDetails = mkASGDetails(newAsgInstances)

    val instanceInfo = Map.empty[String, Ec2InstanceDetails]

    val newData = runMkNewDataMergerSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    val newSlottedASGs = expectedMerge(
      newAsgDetails,
      List.empty[SlottedInstanceDetails]
      // should only have an empty list if the instances went away.
    )
    assertSlottedAsgDetails(newData, newSlottedASGs)
  }

  test("load IPv6 address") {
    val instance = loadSlottedInstanceDetails("/SlottedInstanceDetails-0.json")
    assertEquals(instance.ipv6Address, Some("0:0:0:0:0:FFFF:C0A8:0101"))
  }

  test("load without IPv6 address") {
    val instance = loadSlottedInstanceDetails("/SlottedInstanceDetails-1.json")
    assertEquals(instance.ipv6Address, None)
  }

  private def mkEc2(index: Int): Ec2InstanceDetails = {
    Ec2InstanceDetails(
      Some(s"0:0:0:0:0:FFFF:C0A8:010${index}"),
      Some(s"192.168.1.${index}"),
      Some(s"10.0.0.${index}"),
      None,
      Instant.now(),
      s"ami-001",
      "r4.large"
    )
  }

  private def loadASGDetails: List[SlottedInstanceDetails] = {
    List(
      loadSlottedInstanceDetails("/SlottedInstanceDetails-0.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-1.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-2.json")
    )
  }

  private def newASGDetails(
    append: List[AsgInstanceDetails] = List.empty
  ): List[AsgInstanceDetails] = {
    List(
      AsgInstanceDetails("i-001", "us-west-2b", "InService"),
      AsgInstanceDetails("i-002", "us-west-2a", "InService"),
      AsgInstanceDetails("i-003", "us-west-2b", "InService")
    ) ++ append
  }

  private def mkSlottedASGDetails(
    instances: List[SlottedInstanceDetails],
    instant: Instant = Instant.now(),
    desired: Int = 3,
    max: Int = 6
  ): SlottedAsgDetails = {
    SlottedAsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app-main-all",
      instant,
      desired,
      max,
      0,
      isDisabled = false,
      instances
    )
  }

  private def mkASGDetails(
    instances: List[AsgInstanceDetails],
    instant: Instant = Instant.now(),
    desired: Int = 3,
    max: Int = 6
  ): AsgDetails = {
    AsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app",
      instant,
      desired,
      max,
      0,
      isDisabled = false,
      instances
    )
  }

  private def assertSlottedAsgDetails(data: String, expected: SlottedAsgDetails): Unit = {
    val actual = Json.decode[SlottedAsgDetails](data)
    assertEquals(actual.name, expected.name)
    assertEquals(actual.cluster, expected.cluster)
    assertEquals(actual.desiredCapacity, expected.desiredCapacity)
    assertEquals(actual.maxSize, expected.maxSize)
    assertEquals(actual.minSize, expected.minSize)
    assertEquals(actual.isDisabled, expected.isDisabled)
    assertSlottedInstances(actual.instances, expected.instances)

    // Hack around JSON serialization encoding at milli's resolution, not micros.
    assertEquals(actual.createdTime.toEpochMilli, expected.createdTime.toEpochMilli)
  }

  private def assertSlottedInstances(
    actual: List[SlottedInstanceDetails],
    expected: List[SlottedInstanceDetails]
  ): Unit = {
    assertEquals(actual.size, expected.size)
    actual.zip(expected).foreach {
      case (a, e) =>
        assertEquals(a.instanceId, e.instanceId)
        assertEquals(a.privateIpAddress, e.privateIpAddress)
        assertEquals(a.publicIpAddress, e.publicIpAddress)
        assertEquals(a.publicDnsName, e.publicDnsName)
        assertEquals(a.slot, e.slot)
        assertEquals(a.launchTime.toEpochMilli, e.launchTime.toEpochMilli)
        assertEquals(a.imageId, e.imageId)
        assertEquals(a.instanceType, e.instanceType)
        assertEquals(a.availabilityZone, e.availabilityZone)
        assertEquals(a.lifecycleState, e.lifecycleState)
    }
  }

  private def toSlottedInstance(
    id: String,
    instances: Map[String, Ec2InstanceDetails],
    az: String,
    state: String,
    slot: Int
  ): SlottedInstanceDetails = {
    val ec2 = instances(id)
    SlottedInstanceDetails(
      id,
      ec2.ipv6Address,
      ec2.privateIpAddress,
      ec2.publicIpAddress,
      ec2.publicDnsName,
      slot,
      ec2.launchTime,
      ec2.imageId,
      ec2.instanceType,
      az,
      state
    )
  }

  private def toEc2(slotted: SlottedInstanceDetails): Ec2InstanceDetails = {
    Ec2InstanceDetails(
      slotted.ipv6Address,
      slotted.privateIpAddress,
      slotted.publicIpAddress,
      slotted.publicDnsName,
      slotted.launchTime,
      slotted.imageId,
      slotted.instanceType
    )
  }

  private def expectedMerge(
    newAsgDetails: AsgDetails,
    instances: List[SlottedInstanceDetails]
  ): SlottedAsgDetails = {
    SlottedAsgDetails(
      newAsgDetails.name,
      newAsgDetails.cluster,
      newAsgDetails.createdTime,
      newAsgDetails.desiredCapacity,
      newAsgDetails.maxSize,
      newAsgDetails.minSize,
      newAsgDetails.isDisabled,
      instances
    )
  }

  private def runMkNewDataMergerSlots(
    oldAsgSlotted: SlottedAsgDetails,
    newAsgDetails: AsgDetails,
    instanceInfo: Map[String, Ec2InstanceDetails],
    shouldHaveError: Boolean = false
  ): String = {
    val registry = new DefaultRegistry()
    val counter = registry.counter("ut")
    val oldData = Json.encode(oldAsgSlotted)
    val newData = mkNewDataMergeSlots(oldData, newAsgDetails, instanceInfo, counter)

    if (shouldHaveError) {
      assertEquals(counter.count(), 1L)
      assertEquals(newData, oldData)
    } else {
      assertEquals(counter.count(), 0L)
      assertNotEquals(newData, oldData)
    }
    newData
  }
}
