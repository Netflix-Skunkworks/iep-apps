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
package com.netflix.atlas.persistence

import munit.FunSuite
import java.io.File

class S3CopyUtilsSuite extends FunSuite {

  test("isInactive returns false if lastModified = 0") {
    val file = new File("foo") {
      override def lastModified(): Long = 0
    }
    assert(!S3CopyUtils.isInactive(file, 1000, 12345L))
  }

  test("isInactive returns true if file is old") {
    val file = new File("foo") {
      override def lastModified(): Long = 1000
    }
    assert(S3CopyUtils.isInactive(file, 500, now = 2000))
  }

  test("shouldProcess returns false if file is in activeFiles") {
    val file = new File("bar")
    assert(!S3CopyUtils.shouldProcess(file, Set("bar"), 1000, _ => false))
  }

  test("shouldProcess returns true for inactive temp file") {
    val file = new File("baz.tmp") {
      override def lastModified(): Long = 1000
    }
    assert(S3CopyUtils.shouldProcess(file, Set.empty, 500, _ => true))
  }

  test("shouldProcess returns false for active temp file") {
    val file = new File("baz.tmp") {
      override def lastModified(): Long = 0
    }
    // not inactive yet
    assert(!S3CopyUtils.shouldProcess(file, Set.empty, 500, _ => true))
  }

  test("shouldProcess returns true for non-temp file") {
    val file = new File("baz.data")
    assert(S3CopyUtils.shouldProcess(file, Set.empty, 500, _ => false))
  }

  test("buildS3Key produces expected key structure") {
    val fileName = "2020-05-10T0300.i-localhost.1.XkvU3A.1200-1320"
    val key = S3CopyUtils.buildS3Key(fileName, "atlas", 15)
    assert(key.contains("atlas/2020-05-10T0300"))
    assert(key.endsWith("/i-localhost.1.XkvU3A.1200-1320"))
  }

  test("hash returns 3-char hex prefix and original path") {
    val path = "atlas/2020-01-01T0000"
    val hashed = S3CopyUtils.hash(path)
    val parts = hashed.split("/", 2)
    assert(parts(0).length == 3)
    assert(parts(1).startsWith("atlas/"))
  }

  test("extractMinuteRange handles tmp and range files") {
    assertEquals(S3CopySink.extractMinuteRange("abc.tmp"), "61-61")
    assertEquals(S3CopySink.extractMinuteRange("abc.1200-1300"), "20-21")
    assertEquals(S3CopySink.extractMinuteRange("abc.0000-0123"), "00-02")
  }
}
