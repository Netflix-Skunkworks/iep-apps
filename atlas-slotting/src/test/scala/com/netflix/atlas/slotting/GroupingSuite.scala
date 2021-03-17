/*
 * Copyright 2014-2021 Netflix, Inc.
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
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup
import software.amazon.awssdk.services.autoscaling.model.{Instance => AsgInstance}
import software.amazon.awssdk.services.ec2.model.{Instance => Ec2Instance}

import scala.jdk.CollectionConverters._
import scala.io.Source

class GroupingSuite extends AnyFunSuite with Grouping {

  test("get app") {
    val res = List(
      "atlas_app-v001",
      "atlas_app-main-v002",
      "atlas_app-main-all-v003"
    ).map(getApp)

    assert(res === List("atlas_app", "atlas_app", "atlas_app"))
  }

  test("get cluster") {
    val res = List(
      "atlas_app-v001",
      "atlas_app-main-v002",
      "atlas_app-main-all-v003"
    ).map(getCluster)

    assert(res === List("atlas_app", "atlas_app-main", "atlas_app-main-all"))
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

    assert(asgDetails.name === "atlas_app-main-all-v001")
    assert(asgDetails.cluster === "atlas_app-main-all")
    assert(asgDetails.desiredCapacity === 3)
    assert(asgDetails.maxSize === 6)
    assert(asgDetails.minSize === 0)
    assert(asgDetails.instances.map(_.instanceId) === List("i-001", "i-002", "i-003"))
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

    assert(details.imageId === "ami-001")
    assert(details.instanceType === "r4.large")
    assert(details.privateIpAddress === "192.168.1.1")
  }

  test("make slotted instance details") {
    val instant = Instant.now()

    val asgInstances = List(
      AsgInstanceDetails("i-001", "us-west-2b", "InService"),
      AsgInstanceDetails("i-002", "us-west-2a", "InService"),
      AsgInstanceDetails("i-003", "us-west-2b", "InService")
    )

    val asgDetails = AsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app",
      instant,
      3,
      6,
      0,
      asgInstances
    )

    val instanceInfo = Map(
      "i-001" -> Ec2InstanceDetails("192.168.1.1", None, None, instant, "ami-001", "r4.large"),
      "i-002" -> Ec2InstanceDetails("192.168.1.2", None, None, instant, "ami-001", "r4.large"),
      "i-003" -> Ec2InstanceDetails("192.168.1.3", None, None, instant, "ami-001", "r4.large")
    )

    val slotted = mkSlottedInstanceDetailsList(asgDetails, instanceInfo)

    assert(slotted.map(_.instanceId) === List("i-001", "i-002", "i-003"))
    assert(slotted.map(_.privateIpAddress) === List("192.168.1.1", "192.168.1.2", "192.168.1.3"))
    assert(slotted.map(_.slot) === List(-1, -1, -1))
  }

  test("slotted asg details - too many instances") {
    val instances = List(
      loadSlottedInstanceDetails("/SlottedInstanceDetails-0.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-1.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-2.json")
    )

    val caught = intercept[IllegalArgumentException] {
      SlottedAsgDetails(
        "atlas_app-main-all-v001",
        "atlas_app-main-all",
        Instant.now(),
        2,
        4,
        0,
        instances
      )
    }

    assert(caught.getMessage === "requirement failed: instances.size (3) > desiredCapacity (2)")
  }

  test("assign slots") {
    val instant = Instant.now()

    val asgInstances = List(
      AsgInstanceDetails("i-001", "us-west-2b", "InService"),
      AsgInstanceDetails("i-002", "us-west-2a", "InService"),
      AsgInstanceDetails("i-003", "us-west-2b", "InService")
    )

    val asgDetails = AsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app",
      instant,
      3,
      6,
      0,
      asgInstances
    )

    val instanceInfo = Map(
      "i-001" -> Ec2InstanceDetails("192.168.1.1", None, None, instant, "ami-001", "r4.large"),
      "i-002" -> Ec2InstanceDetails("192.168.1.2", None, None, instant, "ami-001", "r4.large"),
      "i-003" -> Ec2InstanceDetails("192.168.1.3", None, None, instant, "ami-001", "r4.large")
    )

    val slotted = assignSlots(asgDetails, instanceInfo)

    assert(slotted.map(_.instanceId) === List("i-001", "i-002", "i-003"))
    assert(slotted.map(_.privateIpAddress) === List("192.168.1.1", "192.168.1.2", "192.168.1.3"))
    assert(slotted.map(_.slot) === List(0, 1, 2))
  }

  private def loadSlottedInstanceDetails(resource: String): SlottedInstanceDetails = {
    val source = Source.fromURL(getClass.getResource(resource))
    val instance = Json.decode[SlottedInstanceDetails](source.mkString)
    source.close
    instance
  }

  test("merge slots") {
    val instant = Instant.now()

    val oldAsgInstances = List(
      loadSlottedInstanceDetails("/SlottedInstanceDetails-0.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-1.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-2.json")
    )

    val oldAsgSlotted = SlottedAsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app-main-all",
      instant,
      3,
      6,
      0,
      oldAsgInstances
    )

    val newAsgInstances = List(
      AsgInstanceDetails("i-001", "us-west-2b", "InService"),
      AsgInstanceDetails("i-003", "us-west-2b", "InService"),
      AsgInstanceDetails("i-004", "us-west-2a", "InService")
    )

    val newAsgDetails = AsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app",
      instant,
      3,
      6,
      0,
      newAsgInstances
    )

    val instanceInfo = Map(
      "i-001" -> Ec2InstanceDetails("192.168.1.1", None, None, instant, "ami-001", "r4.large"),
      "i-003" -> Ec2InstanceDetails("192.168.1.3", None, None, instant, "ami-001", "r4.large"),
      "i-004" -> Ec2InstanceDetails("192.168.1.4", None, None, instant, "ami-001", "r4.large")
    )

    val merged = mergeSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    assert(merged.map(_.instanceId) === List("i-001", "i-004", "i-003"))
    assert(merged.map(_.privateIpAddress) === List("192.168.1.1", "192.168.1.4", "192.168.1.3"))
    assert(merged.map(_.slot) === List(0, 1, 2))
  }

  test("merge slots - too many instances") {
    val instant = Instant.now()

    val oldAsgInstances = List(
      loadSlottedInstanceDetails("/SlottedInstanceDetails-0.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-1.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-2.json")
    )

    val oldAsgSlotted = SlottedAsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app-main-all",
      instant,
      3,
      6,
      0,
      oldAsgInstances
    )

    val newAsgInstances = List(
      AsgInstanceDetails("i-001", "us-west-2b", "InService"),
      AsgInstanceDetails("i-003", "us-west-2b", "InService"),
      AsgInstanceDetails("i-004", "us-west-2a", "InService"),
      AsgInstanceDetails("i-005", "us-west-2a", "InService")
    )

    val newAsgDetails = AsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app",
      instant,
      3,
      6,
      0,
      newAsgInstances
    )

    val instanceInfo = Map(
      "i-001" -> Ec2InstanceDetails("192.168.1.1", None, None, instant, "ami-001", "r4.large"),
      "i-003" -> Ec2InstanceDetails("192.168.1.3", None, None, instant, "ami-001", "r4.large"),
      "i-004" -> Ec2InstanceDetails("192.168.1.4", None, None, instant, "ami-001", "r4.large"),
      "i-005" -> Ec2InstanceDetails("192.168.1.5", None, None, instant, "ami-001", "r4.large")
    )

    val caught = intercept[NoSuchElementException] {
      mergeSlots(oldAsgSlotted, newAsgDetails, instanceInfo)
    }

    assert(caught.getMessage === "key not found: i-005")
  }

  test("merge slots - too many instances, no entry in instanceInfo") {
    val instant = Instant.now()

    val oldAsgInstances = List(
      loadSlottedInstanceDetails("/SlottedInstanceDetails-0.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-1.json"),
      loadSlottedInstanceDetails("/SlottedInstanceDetails-2.json")
    )

    val oldAsgSlotted = SlottedAsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app-main-all",
      instant,
      3,
      6,
      0,
      oldAsgInstances
    )

    val newAsgInstances = List(
      AsgInstanceDetails("i-001", "us-west-2b", "InService"),
      AsgInstanceDetails("i-003", "us-west-2b", "InService"),
      AsgInstanceDetails("i-004", "us-west-2a", "InService"),
      AsgInstanceDetails("i-005", "us-west-2a", "Terminating")
    )

    val newAsgDetails = AsgDetails(
      "atlas_app-main-all-v001",
      "atlas_app",
      instant,
      3,
      6,
      0,
      newAsgInstances
    )

    val instanceInfo = Map(
      "i-001" -> Ec2InstanceDetails("192.168.1.1", None, None, instant, "ami-001", "r4.large"),
      "i-003" -> Ec2InstanceDetails("192.168.1.3", None, None, instant, "ami-001", "r4.large"),
      "i-004" -> Ec2InstanceDetails("192.168.1.4", None, None, instant, "ami-001", "r4.large")
    )

    val merged = mergeSlots(oldAsgSlotted, newAsgDetails, instanceInfo)

    assert(merged.map(_.instanceId) === List("i-001", "i-004", "i-003"))
    assert(merged.map(_.privateIpAddress) === List("192.168.1.1", "192.168.1.4", "192.168.1.3"))
    assert(merged.map(_.slot) === List(0, 1, 2))
  }
}
