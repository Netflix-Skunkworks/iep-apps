/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.iep.loadgen

import java.time.Duration

import munit.FunSuite

class LoadGenServiceSuite extends FunSuite {
  test("extract step from uri") {
    val actual = LoadGenService.extractStep("/graph?q=name,foo,:eq&step=60s")
    assertEquals(actual, Some(Duration.ofSeconds(60)))
  }

  test("extract step from uri, not present") {
    val actual = LoadGenService.extractStep("/graph?q=name,foo,:eq")
    assertEquals(actual, None)
  }

  test("extract step from uri, invalid uri") {
    val actual = LoadGenService.extractStep("/graph?q=name,{{ .SpinnakerApp }},:eq")
    assertEquals(actual, None)
  }

  test("extract step from uri, invalid step") {
    val actual = LoadGenService.extractStep("/graph?q=name,foo,:eq&step=bad")
    assertEquals(actual, None)
  }
}
