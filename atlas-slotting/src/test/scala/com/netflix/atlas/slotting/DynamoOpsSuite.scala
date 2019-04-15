/*
 * Copyright 2014-2019 Netflix, Inc.
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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.Duration

import org.scalatest.FunSuite

class DynamoOpsSuite extends FunSuite with DynamoOps {

  def mkByteBuffer(s: String): ByteBuffer = {
    val bytes = s.getBytes("UTF-8")
    val baos = new ByteArrayOutputStream(bytes.length)
    baos.write(bytes)
    baos.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  test("compress and decompress") {
    val input = "Atlas Slotting Service"
    val compressed = compress(input)
    assert(input === decompress(compressed))
  }

  test("scan active items") {
    val scanSpec = scanActiveItems()
    assert(scanSpec.getFilterExpression === "#a = :v1")
    assert(scanSpec.getNameMap.toString === s"{#a=$Active}")
    assert(scanSpec.getValueMap.toString === "{:v1=true}")
  }

  test("scan old items") {
    val scanSpec = scanOldItems(Duration.ofDays(1))
    assert(scanSpec.getFilterExpression === "#t < :v1")
    assert(scanSpec.getProjectionExpression === "#n")
    assert(scanSpec.getNameMap.toString === s"{#n=$Name, #t=$Timestamp}")
  }

  test("put slotted asg") {
    val newData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val item = putSlottedAsg("atlas_app-main-all-v001", newData)
    assert(item.hasAttribute(Name))
    assert(item.hasAttribute(Data))
    assert(item.hasAttribute(Active))
    assert(item.hasAttribute(Timestamp))
  }

  test("update slotted asg") {
    val oldData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val newData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 6}""")
    val updateSpec = updateSlottedAsg("atlas_app-main-all-v001", oldData, newData)
    assert(updateSpec.getConditionExpression === "#d = :v1")
    assert(updateSpec.getUpdateExpression === s"set #d = :v2, #a = :v3, #t = :v4")
    assert(updateSpec.getNameMap.toString === s"{#d=data, #a=$Active, #t=$Timestamp}")
  }

  test("update timestamp") {
    val oldData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val updateSpec = updateTimestamp("atlas_app-main-all-v001", oldData)
    assert(updateSpec.getConditionExpression === "#d = :v1")
    assert(updateSpec.getUpdateExpression === s"set #a = :v2, #t = :v3")
    assert(updateSpec.getNameMap.toString === s"{#d=data, #a=$Active, #t=$Timestamp}")
  }

  test("deactivate asg") {
    val oldData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val updateSpec = deactivateAsg("atlas_app-main-all-v001", oldData)
    assert(updateSpec.getConditionExpression === "#d = :v1")
    assert(updateSpec.getUpdateExpression === s"set #a = :v2, #t = :v3")
    assert(updateSpec.getNameMap.toString === s"{#d=data, #a=$Active, #t=$Timestamp}")
  }
}
