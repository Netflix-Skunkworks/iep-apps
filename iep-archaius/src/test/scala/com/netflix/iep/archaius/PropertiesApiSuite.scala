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
package com.netflix.iep.archaius

import java.io.StringReader
import java.util.Properties
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.akka.testkit.MUnitRouteSuite
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock

class PropertiesApiSuite extends MUnitRouteSuite {
  import scala.concurrent.duration._
  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val clock = new ManualClock()
  val registry = new DefaultRegistry(clock)
  val propContext = new PropertiesContext(registry)
  val endpoint = new PropertiesApi(propContext, system)
  val routes = RequestHandler.standardOptions(endpoint.routes)

  private def assertJsonContentType(response: HttpResponse): Unit = {
    assertEquals(response.entity.contentType.mediaType, MediaTypes.`application/json`)
  }

  private def assertResponse(response: HttpResponse, expected: StatusCode): Unit = {
    assertEquals(response.status, expected)
    assertJsonContentType(response)
  }

  test("no asg") {
    Get("/api/v1/property") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
    }
  }

  test("empty") {
    propContext.update(Nil)
    Get("/api/v1/property?asg=foo-main-v001") ~>
    addHeader(Accept(MediaTypes.`application/json`)) ~>
    routes ~>
    check {
      assertResponse(response, StatusCodes.OK)
      assertEquals(responseAs[String], "[]")
    }
  }

  test("properties response") {
    propContext.update(
      List(
        PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L),
        PropertiesApi.Property("foo-main::1", "foo-main", "1", "2", 12345L),
        PropertiesApi.Property("bar-main::c", "bar-main", "c", "d", 12345L)
      )
    )
    Get("/api/v1/property?asg=foo-main-v001") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      val props = new Properties
      props.load(new StringReader(responseAs[String]))
      assertEquals(props.size, 2)
      assertEquals(props.getProperty("a"), "b")
      assertEquals(props.getProperty("1"), "2")
    }
  }

  test("json response") {
    propContext.update(
      List(
        PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L)
      )
    )
    Get("/api/v1/property?asg=foo-main-v001") ~>
    addHeader(Accept(MediaTypes.`application/json`)) ~>
    routes ~>
    check {
      assertResponse(response, StatusCodes.OK)
      val props = Json.decode[List[PropertiesApi.Property]](responseAs[String])
      assertEquals(props, List(PropertiesApi.Property("foo-main::a", "foo-main", "a", "b", 12345L)))
    }
  }
}
