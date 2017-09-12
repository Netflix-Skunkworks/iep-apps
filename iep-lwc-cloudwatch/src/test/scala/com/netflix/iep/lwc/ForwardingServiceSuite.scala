/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.iep.lwc

import org.scalatest.FunSuite

class ForwardingServiceSuite extends FunSuite {

  import ForwardingService._

  private val version =
    """{"ts":1505226236957,"hash":"2d4d"}"""

  private val versionWithUser =
    """{"ts":1505226236957,"hash":"2d4d","user":"brh","comment":"update"}"""

  private val expr =
    """name,http.req.complete,:eq,statistic,count,:eq,:and,:sum,(,nf.account,nf.asg,),:by"""

  private val payload =
    s"""{"email":"bob@example.com","expressions":[{"atlasUri":"$expr","account":"$$(nf.account)","metricName":"requestsPerSecond","dimensions":[{"name":"AutoScalingGroupName","value":"$$(nf.asg)"}]}]}"""

  test("parse update response") {
    val sample = s"""cluster->{"version":$versionWithUser,"payload":$payload}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse(
      ConfigBinVersion(1505226236957L, "2d4d", Some("brh"), Some("update")),
      ClusterConfig("bob@example.com", List(
        ForwardingExpression(
          atlasUri = expr,
          account = "$(nf.account)",
          metricName = "requestsPerSecond",
          dimensions = List(
            ForwardingDimension("AutoScalingGroupName", "$(nf.asg)")
          ))
      ))
    )

    assert(msg.cluster === "cluster")
    assert(msg.response.isUpdate)
    assert(msg.response === expectedResponse)
  }

  test("parse delete response") {
    val sample = s"""cluster->{"version":$version,"payload":""}"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse.delete(ConfigBinVersion(1505226236957L, "2d4d"))

    assert(msg.cluster === "cluster")
    assert(msg.response.isDelete)
    assert(msg.response === expectedResponse)
  }

  test("parse heartbeat") {
    val sample = s"""heartbeat->1,i-12345,1a7fb69e-005c-4ebc-bf05-aba3386c682f,1"""
    val msg = Message(sample)

    val expectedResponse = ConfigBinResponse.delete(ConfigBinVersion(1505226236957L, "2d4d"))

    assert(msg.cluster === "heartbeat")
    assert(msg.isHeartbeat)
  }
}
