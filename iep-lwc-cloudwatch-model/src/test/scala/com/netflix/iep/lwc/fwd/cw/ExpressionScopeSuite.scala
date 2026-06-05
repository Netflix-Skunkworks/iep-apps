/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.iep.lwc.fwd.cw

import java.util.regex.Pattern
import munit.FunSuite

class ExpressionScopeSuite extends FunSuite {

  import ExpressionScope.*

  // Simulates the legacy filter regex after HOCON substitution for us-east-1/test
  private val legacyFilter = Pattern.compile(".*atlas-foo[.]us-east-1[.]test[.]example[.]com.*")
  private val instanceRegion = "us-east-1"
  private val instanceEnv = "test"

  //
  // effectiveRegionEnv tests
  //

  test("effectiveRegionEnv: regional namespace") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-east-1.test"
    assertEquals(effectiveRegionEnv(uri), Some((Some("us-east-1"), "test")))
  }

  test("effectiveRegionEnv: global namespace no region") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test"
    assertEquals(effectiveRegionEnv(uri), Some((None, "test")))
  }

  test("effectiveRegionEnv: service name with hyphens") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=iep-us-east-1.prod"
    assertEquals(effectiveRegionEnv(uri), Some((Some("us-east-1"), "prod")))
  }

  test("effectiveRegionEnv: no ns param") {
    val uri = "http://atlas-foo.us-east-1.test.example.com/api/v1/graph?q=name,cpu,:eq,:sum"
    assertEquals(effectiveRegionEnv(uri), None)
  }

  test("effectiveRegionEnv: region from cq= when not in namespace") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.prod&cq=nf.region,us-east-1,:eq"
    assertEquals(effectiveRegionEnv(uri), Some((Some("us-east-1"), "prod")))
  }

  test("effectiveRegionEnv: region from q= when not in namespace or cq") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,nf.region,us-east-1,:eq,:and,:sum&ns=main.prod"
    assertEquals(effectiveRegionEnv(uri), Some((Some("us-east-1"), "prod")))
  }

  test("effectiveRegionEnv: no region in namespace, cq, or q is global") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.prod"
    assertEquals(effectiveRegionEnv(uri), Some((None, "prod")))
  }

  //
  // matchesInstance tests
  //

  test("matchesInstance: legacy URI passes through unchanged") {
    val uri = "http://atlas-foo.us-east-1.test.example.com/api/v1/graph?q=name,cpu,:eq,:sum"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: legacy URI wrong region drops") {
    val uri = "http://atlas-foo.us-east-2.test.example.com/api/v1/graph?q=name,cpu,:eq,:sum"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= regional match") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-east-1.test"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= regional region mismatch") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-east-2.test"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= regional env mismatch") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-east-1.prod"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global namespace is not supported") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global namespace drops regardless of region") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test"
    assert(!matchesInstance(uri, legacyFilter, "us-west-2", instanceEnv))
  }

  test("matchesInstance: no ns= and no legacy match drops") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global with cq= region match") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test&cq=nf.region,us-east-1,:eq"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global with cq= region mismatch") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test&cq=nf.region,us-west-2,:eq"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global with q= region match") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,nf.region,us-east-1,:eq,:and,:sum&ns=main.test"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }
}
