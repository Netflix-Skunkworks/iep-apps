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
package com.netflix.atlas.cloudwatch

import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.regions.Region

class ConfigAccountSupplierSuite extends FunSuite {

  val rules = new CloudWatchRules(ConfigFactory.load())

  test("test config") {
    val cfg = ConfigFactory.load()
    val accts = new ConfigAccountSupplier(cfg, rules)
    val map = accts.accounts
    assertEquals(map.size, 3)
    assertEquals(map("000000000001"), accts.defaultRegions.map(r => r -> accts.namespaces).toMap)
    assertEquals(map("000000000002"), accts.defaultRegions.map(r => r -> accts.namespaces).toMap)
    assertEquals(map("000000000003"), Map(Region.US_EAST_1 -> accts.namespaces))
  }

  test("empty accounts") {
    val cfg = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.polling {
        |  default-regions = ["us-east-1"]
        |  accounts = []
        |}
      """.stripMargin)
    val accts = new ConfigAccountSupplier(cfg, rules)
    assert(accts.accounts.isEmpty)
  }

  test("missing accounts") {
    val cfg = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.polling {
        |  default-regions = ["us-east-1"]
        |}
      """.stripMargin)
    intercept[Missing] {
      new ConfigAccountSupplier(cfg, rules)
    }
  }
}
