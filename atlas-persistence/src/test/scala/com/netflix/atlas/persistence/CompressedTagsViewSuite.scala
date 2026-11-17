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

import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

class CompressedTagsViewSuite extends FunSuite {

  private val compressed =
    CompressedTagsView.compressedStrings(Map("name" -> 0, "node" -> 1, "cpu" -> 2))

  private val tags = Map("name" -> "cpu", "node" -> "i-123", "app" -> "foo")

  private val expected = new java.util.HashMap[String, String]()
  expected.put("\u0080", "\u0082")
  expected.put("\u0081", "i-123")
  expected.put("app", "foo")

  test("compressedStrings") {
    assertEquals(compressed.get("name"), "\u0080")
    assertEquals(compressed.get("node"), "\u0081")
    assertEquals(compressed.get("cpu"), "\u0082")
    assertEquals(compressed.size(), 3)
  }

  test("sorted tag map") {
    val view = new CompressedTagsView(SortedTagMap(tags), compressed)
    assertEquals(view.size(), 3)
    assertEquals(view: java.util.Map[String, String], expected)
  }

  test("generic map") {
    val view = new CompressedTagsView(tags, compressed)
    assertEquals(view.size(), 3)
    assertEquals(view: java.util.Map[String, String], expected)
  }

  test("empty") {
    val view = new CompressedTagsView(SortedTagMap.empty, compressed)
    assertEquals(view.size(), 0)
    assert(!view.entrySet().iterator().hasNext)
  }

  test("iterator past end") {
    val it = new CompressedTagsView(SortedTagMap(tags), compressed).entrySet().iterator()
    (0 until 3).foreach(_ => it.next())
    intercept[NoSuchElementException](it.next())
  }

  test("iterator past end, generic map") {
    val it = new CompressedTagsView(tags, compressed).entrySet().iterator()
    (0 until 3).foreach(_ => it.next())
    intercept[NoSuchElementException](it.next())
  }

  test("compressedStrings is unmodifiable") {
    intercept[UnsupportedOperationException](compressed.put("foo", "bar"))
  }
}
