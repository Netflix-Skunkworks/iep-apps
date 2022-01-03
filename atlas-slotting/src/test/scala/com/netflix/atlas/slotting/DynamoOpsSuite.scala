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
package com.netflix.atlas.slotting

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration

import munit.FunSuite

class DynamoOpsSuite extends FunSuite with DynamoOps {

  def mkByteBuffer(s: String): ByteBuffer = {
    ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
  }

  test("compress and decompress") {
    val input = "Atlas Slotting Service"
    val compressed = Util.compress(input)
    assertEquals(input, Util.decompress(compressed))
  }

  test("active items spec") {
    val scanSpec = activeItemsScanRequest("test")
    assertEquals(scanSpec.filterExpression, "#a = :v1")
    assertEquals(scanSpec.expressionAttributeNames.toString, s"{#a=$Active}")
    assertEquals(scanSpec.expressionAttributeValues.toString, "{:v1=AttributeValue(BOOL=true)}")
  }

  test("old items spec") {
    val scanSpec = oldItemsScanRequest("test", Duration.ofDays(1))
    assertEquals(scanSpec.filterExpression, "#t < :v1")
    assertEquals(scanSpec.projectionExpression, "#n")
    assertEquals(scanSpec.expressionAttributeNames.toString, s"{#n=$Name, #t=$Timestamp}")
  }

  test("new asg item") {
    val newData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val item = putAsgItemRequest("test", "atlas_app-main-all-v001", newData).item()
    assert(item.containsKey(Name))
    assert(item.containsKey(Data))
    assert(item.containsKey(Active))
    assert(item.containsKey(Timestamp))
  }

  test("update asg spec") {
    val oldData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 3}""")
    val newData = mkByteBuffer("""{"name": "atlas_app-main-all-v001", "desiredCapacity": 6}""")
    val updateSpec = updateAsgItemRequest("test", "atlas_app-main-all-v001", oldData, newData)
    assertEquals(updateSpec.conditionExpression, "#d = :v1")
    assertEquals(updateSpec.updateExpression, s"set #d = :v2, #a = :v3, #t = :v4")
    assertEquals(
      updateSpec.expressionAttributeNames.toString,
      s"{#a=$Active, #d=data, #t=$Timestamp}"
    )
  }

  test("update timestamp spec") {
    val updateSpec = updateTimestampItemRequest("test", "atlas_app-main-all-v001", 1556568270713L)
    assertEquals(updateSpec.conditionExpression, "#t = :v1")
    assertEquals(updateSpec.updateExpression, s"set #a = :v2, #t = :v3")
    assertEquals(updateSpec.expressionAttributeNames.toString, s"{#a=$Active, #t=$Timestamp}")
  }

  test("deactivate asg spec") {
    val updateSpec = deactivateAsgItemRequest("test", "atlas_app-main-all-v001")
    assertEquals(updateSpec.conditionExpression, "#a = :v1")
    assertEquals(updateSpec.updateExpression, s"set #a = :v2, #t = :v3")
    assertEquals(updateSpec.expressionAttributeNames.toString, s"{#a=$Active, #t=$Timestamp}")
  }
}
