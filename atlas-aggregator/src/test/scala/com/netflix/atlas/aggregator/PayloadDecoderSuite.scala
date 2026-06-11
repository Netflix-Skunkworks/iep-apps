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
package com.netflix.atlas.aggregator

import com.netflix.atlas.json3.Json
import com.netflix.spectator.api.Id
import munit.FunSuite

import scala.collection.mutable

class PayloadDecoderSuite extends FunSuite {

  private class CapturingAggregator extends Aggregator {

    val ids: mutable.ArrayBuffer[Id] = mutable.ArrayBuffer.empty[Id]
    override def add(id: Id, value: Double): Unit = ids += id
    override def max(id: Id, value: Double): Unit = ids += id
  }

  private def valueForKey(id: Id, key: String): String = {
    var result: String = null
    val n = id.size()
    var i = 0
    while (i < n) {
      if (id.getKey(i) == key) result = id.getValue(i)
      i += 1
    }
    result
  }

  // Single counter update: name=cpu_user, app=www
  private def payload() =
    Json.newJsonParser(
      Json.encode(List(4, "name", "cpu_user", "app", "www", 2, 0, 1, 2, 3, 0, 1.0))
    )

  test("internal decoder canonicalizes tag values across separate payloads") {
    val agg = new CapturingAggregator
    // Two independent decode calls produce independent string tables; only the shared
    // string cache can make equal values resolve to the same instance.
    PayloadDecoder.internal.decode(payload(), agg)
    PayloadDecoder.internal.decode(payload(), agg)

    assertEquals(agg.ids.size, 2)
    val v0 = valueForKey(agg.ids(0), "app")
    val v1 = valueForKey(agg.ids(1), "app")
    assertEquals(v0, "www")
    assertEquals(v1, "www")
    assert(v0 ne null)
    assert(
      v0 eq v1,
      "equal tag values from separate payloads should be the same interned instance"
    )
  }
}
