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
package com.netflix.atlas.persistence

import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class S3CopySinkSuite extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterAll {
  test("extractMinuteRange") {
    assert(S3CopySink.extractMinuteRange("abc.tmp") === "61-61")
    assert(S3CopySink.extractMinuteRange("abc.1200-1300") === "20-21")
    assert(S3CopySink.extractMinuteRange("abc.0000-0123") === "00-02")
  }
}
