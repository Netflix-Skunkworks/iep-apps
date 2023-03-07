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
package com.netflix.atlas.util

import munit.FunSuite

class XXHasherSuite extends FunSuite {

  test("hash byte array") {
    assertEquals(XXHasher.hash("Hello World".getBytes("UTF-8")), 7148569436472236994L)
  }

  test("hash byte array empty") {
    assertEquals(XXHasher.hash(new Array[Byte](0)), -1205034819632174695L)
  }

  test("hash byte array null") {
    intercept[NullPointerException] {
      XXHasher.hash(null.asInstanceOf[Array[Byte]])
    }
  }

  test("hash byte array offset") {
    assertEquals(XXHasher.hash("Hello World".getBytes("UTF-8"), 1, 5), -4877171975935371781L)
  }

  test("hash byte string") {
    assertEquals(XXHasher.hash("Hello World"), -7682288509216370722L)
  }

  test("updateHash byte array") {
    val hash = XXHasher.hash("Hello World".getBytes("UTF-8"))
    assertEquals(XXHasher.updateHash(hash, " from Atlas!".getBytes("UTF-8")), 6820347041909772079L)
  }

  test("updateHash byte array offset") {
    val hash = XXHasher.hash("Hello World".getBytes("UTF-8"))
    assertEquals(
      XXHasher.updateHash(hash, " from Atlas!".getBytes("UTF-8"), 1, 8),
      -2548206119163337765L
    )
  }

  test("updateHash string") {
    val hash = XXHasher.hash("Hello World")
    assertEquals(XXHasher.updateHash(hash, " from Atlas!"), -5099163101293074982L)
  }

  test("combineHashes") {
    val hashA = XXHasher.hash("Hello World")
    val hashB = XXHasher.hash(" from Atlas!")
    assertEquals(XXHasher.combineHashes(hashA, hashB), -5099163101293074982L)
  }
}
