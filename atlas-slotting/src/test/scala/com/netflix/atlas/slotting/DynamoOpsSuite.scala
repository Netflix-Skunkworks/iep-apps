/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas.slotting

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration

import org.scalatest.funsuite.AnyFunSuite

class DynamoOpsSuite extends AnyFunSuite with DynamoOps {

  def mkByteBuffer(s: String): ByteBuffer = {
    ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
  }

  test("compress and decompress") {
    val input = "Atlas Slotting Service"
    val compressed = Util.compress(input)
    assert(input === Util.decompress(compressed))
  }

  test("active items spec") {
    val scanSpec = activeItemsScanSpec()
    assert(scanSpec.getFilterExpression === "#a = :v1")
    assert(scanSpec.getNameMap.toString === s"{#a=$Active}")
    assert(scanSpec.getValueMap.toString === "{:v1=true}")
  }

  test("old items spec") {
    val scanSpec = oldItemsScanSpec(Duration.ofDays(1))
    assert(scanSpec.getFilterExpression === "#t < :v1")
    assert(scanSpec.getProjectionExpression === "#n")
    assert(scanSpec.getNameMap.toString === s"{#n=$Name, #t=$Timestamp}")
  }

  test("new asg item") {
    val newData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val item = newAsgItem("atlas_app-main-all-v001", newData)
    assert(item.hasAttribute(Name))
    assert(item.hasAttribute(Data))
    assert(item.hasAttribute(Active))
    assert(item.hasAttribute(Timestamp))
  }

  test("update asg spec") {
    val oldData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val newData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 6}""")
    val updateSpec = updateAsgItemSpec("atlas_app-main-all-v001", oldData, newData)
    assert(updateSpec.getConditionExpression === "#d = :v1")
    assert(updateSpec.getUpdateExpression === s"set #d = :v2, #a = :v3, #t = :v4")
    assert(updateSpec.getNameMap.toString === s"{#d=data, #a=$Active, #t=$Timestamp}")
  }

  test("update timestamp spec") {
    val updateSpec = updateTimestampItemSpec("atlas_app-main-all-v001", 1556568270713L)
    assert(updateSpec.getConditionExpression === "#t = :v1")
    assert(updateSpec.getUpdateExpression === s"set #a = :v2, #t = :v3")
    assert(updateSpec.getNameMap.toString === s"{#a=$Active, #t=$Timestamp}")
  }

  test("deactivate asg spec") {
    val updateSpec = deactivateAsgItemSpec("atlas_app-main-all-v001")
    assert(updateSpec.getConditionExpression === "#a = :v1")
    assert(updateSpec.getUpdateExpression === s"set #a = :v2, #t = :v3")
    assert(updateSpec.getNameMap.toString === s"{#a=$Active, #t=$Timestamp}")
  }
}
