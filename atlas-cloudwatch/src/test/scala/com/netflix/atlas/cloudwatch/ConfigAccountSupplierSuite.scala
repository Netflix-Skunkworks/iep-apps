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
package com.netflix.atlas.cloudwatch

import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.regions.Region

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ConfigAccountSupplierSuite extends FunSuite {

  test("test config") {
    val cfg = ConfigFactory.load()
    val accts = new ConfigAccountSupplier(cfg)
    val map = Await.result(accts.accounts, 60.seconds)
    assertEquals(map.size, 3)
    assertEquals(map("000000000001"), accts.defaultRegions)
    assertEquals(map("000000000002"), accts.defaultRegions)
    assertEquals(map("000000000003"), List(Region.US_EAST_1))
  }

  test("empty accounts") {
    val cfg = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.polling {
        |  default-regions = ["us-east-1"]
        |  accounts = []
        |}
      """.stripMargin)
    val accts = new ConfigAccountSupplier(cfg)
    val map = Await.result(accts.accounts, 60.seconds)
    assert(map.isEmpty)
  }

  test("missing accounts") {
    val cfg = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.polling {
        |  default-regions = ["us-east-1"]
        |}
      """.stripMargin)
    intercept[Missing] {
      new ConfigAccountSupplier(cfg)
    }
  }
}
