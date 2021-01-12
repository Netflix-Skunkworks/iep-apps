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
package com.netflix.iep.lwc

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class BridgeApiSuite extends AnyFunSuite with ScalatestRouteTest with BeforeAndAfter {

  import scala.concurrent.duration._
  private implicit val routeTestTimeout = RouteTestTimeout(5.second)

  private val config = ConfigFactory.load()
  private val evaluator = new ExpressionsEvaluator(config)
  private val endpoint = new BridgeApi(config, new NoopRegistry, evaluator, system)
  private val routes = RequestHandler.standardOptions(endpoint.routes)

  before {
    evaluator.clear()
  }

  test("publish no content") {
    Post("/api/v1/publish") ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(responseAs[String] === "")
    }
  }

  test("publish empty object") {
    Post("/api/v1/publish", "{}") ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(responseAs[String] === "")
    }
  }

  test("parse simple batch") {
    val json = s"""{
        "tags": {
          "cluster": "foo",
          "node": "i-123"
        },
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    val datapoints = BridgeApi.decodeBatch(json)
    assert(datapoints.size === 1)
    assert(datapoints.head.id === Id.create("cpuUser").withTags("cluster", "foo", "node", "i-123"))
  }

  test("publish simple batch") {
    val json = s"""{
        "tags": {
          "cluster": "foo",
          "node": "i-123"
        },
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("publish bad json") {
    Post("/api/v1/publish", "fubar") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish invalid object") {
    Post("/api/v1/publish", "{\"foo\":\"bar\"}") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("parse tag value is null") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": null},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    val datapoints = BridgeApi.decodeBatch(json)
    assert(!datapoints.head.id.tags().iterator().hasNext)
  }

  test("publish tag value is null") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": null},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("publish tag value is empty") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": ""},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("parse name is missing") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"no-name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    val datapoints = BridgeApi.decodeBatch(json)
    assert(datapoints.isEmpty)
  }

  test("parse common name only") {
    val json = s"""{
        "tags": {"name": "foo"},
        "metrics": [
          {
            "tags": {"no-name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    val datapoints = BridgeApi.decodeBatch(json)
    assert(datapoints.head.id === Id.create("foo").withTag("no-name", "cpuUser"))
  }

  test("parse conflicting names") {
    val json = s"""{
        "tags": {"name": "foo", "c": "d"},
        "metrics": [
          {
            "tags": {"name": "bar", "a": "b"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    val datapoints = BridgeApi.decodeBatch(json)
    assert(datapoints.head.id === Id.create("bar").withTags("a", "b", "c", "d"))
  }

  test("publish too many tags") {
    val tags = (0 until 20).map(i => s""""$i":"$i"""").mkString(",")
    val json = s"""{
        "tags": {$tags},
        "metrics": [
          {
            "tags": {"name": "cpuUser", $tags},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("parse too many tags") {
    import scala.jdk.CollectionConverters._
    val tags = (0 until 20).map(i => i.toString -> i.toString).toMap
    val tagsString = (0 until 20).map(i => s""""$i":"$i"""").mkString(",")
    val json = s"""{
        "tags": {$tagsString},
        "metrics": [
          {
            "tags": {"name": "cpuUser", $tagsString},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    val datapoints = BridgeApi.decodeBatch(json)
    val id = datapoints.head.id
    val normalizedId = Id.create(id.name()).withTags(id.tags())
    assert(normalizedId === Id.create("cpuUser").withTags(tags.asJava))
  }
}
