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
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", Some("us-east-1"))))
  }

  test("effectiveRegionEnv: global namespace no region") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", None)))
  }

  test("effectiveRegionEnv: service name with hyphens") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=iep-us-east-1.prod"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("prod", Some("us-east-1"))))
  }

  test("effectiveRegionEnv: hyphenated service name with no region is global, not region-scoped") {
    // "api-service" is the service name, not service "api" in region "service"; the segment
    // after the first hyphen is not AWS-region-shaped, so this must resolve to global.
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=api-service.prod"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("prod", None)))
  }

  test("effectiveRegionEnv: legacy host URI extracts region/env from hostname") {
    val uri = "http://atlas-foo.us-east-1.test.example.com/api/v1/graph?q=name,cpu,:eq,:sum"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", Some("us-east-1"))))
  }

  test("effectiveRegionEnv: region from cq= when not in namespace") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.prod&cq=nf.region,us-east-1,:eq"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("prod", Some("us-east-1"))))
  }

  test("effectiveRegionEnv: region from q= when not in namespace or cq") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,nf.region,us-east-1,:eq,:and,:sum&ns=main.prod"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("prod", Some("us-east-1"))))
  }

  test("effectiveRegionEnv: no region in namespace, cq, or q is global") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.prod"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("prod", None)))
  }

  test("effectiveRegionEnv: no ns and non-regional host is unscoped") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum"
    assertEquals(effectiveRegionEnv(uri), None)
  }

  test("effectiveRegionEnv: global ns with no q param is unscoped") {
    val uri = "http://localhost/api/v1/graph?ns=main.test"
    assertEquals(effectiveRegionEnv(uri), None)
  }

  test("effectiveRegionEnv: multiple q expressions with different regions is global") {
    val uri =
      "http://localhost/api/v1/graph?ns=main.test" +
        "&q=name,a,:eq,nf.region,us-east-1,:eq,:and,:sum," +
        "name,b,:eq,nf.region,us-west-2,:eq,:and,:sum"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", None)))
  }

  test("effectiveRegionEnv: multiple q expressions agreeing on one region is regional") {
    val uri =
      "http://localhost/api/v1/graph?ns=main.test" +
        "&q=name,a,:eq,nf.region,us-east-1,:eq,:and,:sum," +
        "name,b,:eq,nf.region,us-east-1,:eq,:and,:sum"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", Some("us-east-1"))))
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
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test&cq=nf.region,us-east-1,:eq"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global with cq= region mismatch") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test&cq=nf.region,us-west-2,:eq"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: ns= global with q= region match") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,nf.region,us-east-1,:eq,:and,:sum&ns=main.test"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: cq= region match but env mismatch drops") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.prod&cq=nf.region,us-east-1,:eq"
    // region us-east-1 matches the instance, but the namespace env (prod) does not
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: q= region match but env mismatch drops") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,nf.region,us-east-1,:eq,:and,:sum&ns=main.prod"
    // region us-east-1 matches the instance, but the namespace env (prod) does not
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: host region/env matched by hostname parsing, not the legacy filter") {
    // legacyFilter only matches the atlas-foo host; a different host with the same
    // region/env still matches via RegionSpecificHostPattern in effectiveRegionEnv
    val uri = "http://atlas-bar.us-east-1.test.example.com/api/v1/graph?q=name,cpu,:eq,:sum"
    assert(matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  test("matchesInstance: legacy host env mismatch drops") {
    val uri = "http://atlas-foo.us-east-1.prod.example.com/api/v1/graph?q=name,cpu,:eq,:sum"
    assert(!matchesInstance(uri, legacyFilter, instanceRegion, instanceEnv))
  }

  //
  // Multiple expressions with different regions
  //

  test("matchesInstance: filters a list of regional ns= expressions to the instance region") {
    val uris = List(
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-east-1.test",
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-west-2.test",
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-eu-west-1.test"
    )
    val matched = uris.filter(matchesInstance(_, legacyFilter, instanceRegion, instanceEnv))
    assertEquals(matched, List(uris.head))
  }

  test("matchesInstance: same ns= expression routed to each region's instance independently") {
    val uri = "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main-us-west-2.test"
    assert(!matchesInstance(uri, legacyFilter, "us-east-1", instanceEnv))
    assert(matchesInstance(uri, legacyFilter, "us-west-2", instanceEnv))
    assert(!matchesInstance(uri, legacyFilter, "eu-west-1", instanceEnv))
  }

  //
  // Region carried as an :in clause (multiple regions for one expression)
  //
  // The region matcher only understands nf.region,<region>,:eq. An :in clause
  // listing multiple regions is not parsed, so the region resolves to None and
  // the expression is treated as global (and therefore dropped).
  //

  test("effectiveRegionEnv: region in :in clause is not extracted (treated as global)") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test" +
        "&cq=nf.region,(,us-east-1,us-west-2,),:in"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", None)))
  }

  test("matchesInstance: region in :in clause drops on every region instance") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test" +
        "&cq=nf.region,(,us-east-1,us-west-2,),:in"
    assert(!matchesInstance(uri, legacyFilter, "us-east-1", instanceEnv))
    assert(!matchesInstance(uri, legacyFilter, "us-west-2", instanceEnv))
  }

  //
  // Multiple regions OR'd together for one expression
  //
  // An expression that spans more than one region has no single region scope, so
  // the region resolves to None and the expression is treated as global (and
  // therefore dropped on every instance).
  //

  test("effectiveRegionEnv: OR of regions resolves to no single region (global)") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test" +
        "&cq=nf.region,us-east-1,:eq,nf.region,us-west-2,:eq,:or"
    assertEquals(effectiveRegionEnv(uri), Some(ExpressionScope("test", None)))
  }

  test("matchesInstance: OR of regions drops on every region instance") {
    val uri =
      "http://localhost/api/v1/graph?q=name,cpu,:eq,:sum&ns=main.test" +
        "&cq=nf.region,us-east-1,:eq,nf.region,us-west-2,:eq,:or"
    assert(!matchesInstance(uri, legacyFilter, "us-east-1", instanceEnv))
    assert(!matchesInstance(uri, legacyFilter, "us-west-2", instanceEnv))
  }
}
