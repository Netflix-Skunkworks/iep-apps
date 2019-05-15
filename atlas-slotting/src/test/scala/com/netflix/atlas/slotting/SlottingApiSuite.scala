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

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Host
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.json.Json
import org.scalatest.FunSuite

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._
import scala.io.Source

case class Message(message: String)

class SlottingApiSuite extends FunSuite with ScalatestRouteTest {
  implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  val slottingCache = new SlottingCache()
  val endpoint = new SlottingApi(slottingCache)
  val routes: Route = RequestHandler.standardOptions(endpoint.routes)

  private def assertResponse(response: HttpResponse, expectedStatus: StatusCode): Unit = {
    assert(response.status === expectedStatus)
    assert(response.entity.contentType.mediaType === MediaTypes.`application/json`)
  }

  private def loadSlottedAsgDetails(resource: String): SlottedAsgDetails = {
    val source = Source.fromURL(getClass.getResource(resource))
    val asg = Json.decode[SlottedAsgDetails](source.mkString)
    source.close
    asg
  }

  test("service description") {
    Get("/") ~> Host("localhost", 7101) ~> routes ~> check {
      val description = Json.decode[Map[String, Any]](responseAs[String])
      val endpoints = description("endpoints").asInstanceOf[List[String]]
      assertResponse(response, StatusCodes.OK)
      assert(description("description") === "Atlas Slotting Service")
      assert(endpoints.size === 7)
    }
  }

  test("bad url parameters (standard api)") {
    val malformed = "MalformedQueryParamRejection(verbose,'foo' is not a valid Boolean value,None)"

    Get("/api/v1/autoScalingGroups?verbose=foo") ~> routes ~> check {
      assertResponse(response, StatusCodes.BadRequest)
      val res = Json.decode[Message](responseAs[String])
      assert(res === Message(malformed))
    }
    Get("/api/v1/autoScalingGroups?foo=bar") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
  }

  test("no cache data (standard api)") {
    Get("/api/v1/autoScalingGroups") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
    Get("/api/v1/autoScalingGroups?verbose=true") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
    Get("/api/v1/autoScalingGroups/atlas_app-main-all-v001") ~> routes ~> check {
      assertResponse(response, StatusCodes.NotFound)
      val res = Json.decode[Message](responseAs[String])
      assert(res === Message("Not Found"))
    }
  }

  test("no cache data (edda api)") {
    Get("/api/v2/group/autoScalingGroups") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
    Get("/api/v2/group/autoScalingGroups;_expand") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
    Get("/api/v2/group/autoScalingGroups/atlas_app-main-all-v001") ~> routes ~> check {
      assertResponse(response, StatusCodes.NotFound)
      val res = Json.decode[Message](responseAs[String])
      assert(res === Message("Not Found"))
    }
    Get("/REST/v2/group/autoScalingGroups") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
    Get("/REST/v2/group/autoScalingGroups;_expand") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List.empty[String])
    }
    Get("/REST/v2/group/autoScalingGroups/atlas_app-main-all-v001") ~> routes ~> check {
      assertResponse(response, StatusCodes.NotFound)
      val res = Json.decode[Message](responseAs[String])
      assert(res === Message("Not Found"))
    }
  }

  test("load cache") {
    slottingCache.asgs = SortedMap(
      "atlas_app-main-all-v001" -> loadSlottedAsgDetails("/atlas_app-main-all-v001.json")
    )

    assert(slottingCache.asgs.size === 1)
  }

  test("cache data (standard api)") {
    Get("/api/v1/autoScalingGroups") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List("atlas_app-main-all-v001"))
    }
    Get("/api/v1/autoScalingGroups?verbose=true") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get("/api/v1/autoScalingGroups/atlas_app-main-all-v001") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[SlottedAsgDetails](responseAs[String])

      assert(res.name === "atlas_app-main-all-v001")
      assert(res.cluster === "atlas_app-main-all")
      assert(res.desiredCapacity === 3)
      assert(res.maxSize === 6)
      assert(res.minSize === 0)
      assert(res.instances.size === 3)

      assert(res.instances.head.instanceId === "i-001")
      assert(res.instances.head.privateIpAddress === "192.168.1.1")
      assert(res.instances.head.publicIpAddress === None)
      assert(res.instances.head.publicDnsName === None)
      assert(res.instances.head.slot === 0)
      assert(res.instances.head.availabilityZone === "us-west-2b")
      assert(res.instances.head.imageId === "ami-001")
      assert(res.instances.head.instanceType === "r4.large")
      assert(res.instances.head.lifecycleState === "InService")
    }
  }

  test("cache data (edda api)") {
    val fieldSelector =
      ":(autoScalingGroupName,instances:(instanceId,slot,lifecycleState,privateIpAddress))"

    Get("/api/v2/group/autoScalingGroups") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List("atlas_app-main-all-v001"))
    }
    Get("/api/v2/group/autoScalingGroups;_expand") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get(s"/api/v2/group/autoScalingGroups;_expand$fieldSelector") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get("/api/v2/group/autoScalingGroups;_pp;_expand") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get(s"/api/v2/group/autoScalingGroups;_pp;_expand$fieldSelector") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get("/api/v2/group/autoScalingGroups/atlas_app-main-all-v001") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[SlottedAsgDetails](responseAs[String])
      assert(res.name === "atlas_app-main-all-v001")
    }
    Get("/REST/v2/group/autoScalingGroups") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[String]](responseAs[String])
      assert(res === List("atlas_app-main-all-v001"))
    }
    Get("/REST/v2/group/autoScalingGroups;_expand") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get(s"/REST/v2/group/autoScalingGroups;_expand$fieldSelector") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get("/REST/v2/group/autoScalingGroups;_pp;_expand") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get(s"/REST/v2/group/autoScalingGroups;_pp;_expand$fieldSelector") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[List[SlottedAsgDetails]](responseAs[String])
      assert(res.map(_.name) === List("atlas_app-main-all-v001"))
    }
    Get("/REST/v2/group/autoScalingGroups/atlas_app-main-all-v001") ~> routes ~> check {
      assertResponse(response, StatusCodes.OK)
      val res = Json.decode[SlottedAsgDetails](responseAs[String])
      assert(res.name === "atlas_app-main-all-v001")
    }
  }
}
