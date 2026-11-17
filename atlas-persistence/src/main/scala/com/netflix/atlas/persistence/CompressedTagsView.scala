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

/**
  * Read-only Java map view over a Scala tag map that replaces common strings with their
  * compressed form as entries are iterated. Used to pass tags to the Avro writer without
  * copying them into a new map for each datapoint. The Avro `GenericDatumWriter` only
  * uses `size()` and `entrySet().iterator()` when writing a map.
  *
  * @param tags
  *     Tags for the datapoint.
  * @param compressedStrings
  *     Mapping from the original string to the compressed form. Strings that are not
  *     present will be used as is.
  */
class CompressedTagsView(
  tags: Map[String, String],
  compressedStrings: java.util.Map[String, String]
) extends java.util.AbstractMap[String, String] {

  private def compress(s: String): String = {
    val c = compressedStrings.get(s)
    if (c == null) s else c
  }

  override def size(): Int = tags.size

  override def entrySet(): java.util.Set[java.util.Map.Entry[String, String]] = {
    new java.util.AbstractSet[java.util.Map.Entry[String, String]] {

      override def size(): Int = tags.size

      override def iterator(): java.util.Iterator[java.util.Map.Entry[String, String]] = {
        tags match {
          case m: SortedTagMap => sortedIterator(m)
          case m               => genericIterator(m)
        }
      }
    }
  }

  private def entry(k: String, v: String): java.util.Map.Entry[String, String] = {
    new java.util.AbstractMap.SimpleImmutableEntry(compress(k), compress(v))
  }

  /** Index based iteration to avoid allocating a tuple for each entry. */
  private def sortedIterator(
    m: SortedTagMap
  ): java.util.Iterator[java.util.Map.Entry[String, String]] = {
    new java.util.Iterator[java.util.Map.Entry[String, String]] {
      private val n = m.size
      private var i = 0

      override def hasNext: Boolean = i < n

      override def next(): java.util.Map.Entry[String, String] = {
        if (i >= n) throw new NoSuchElementException()
        val e = entry(m.key(i), m.value(i))
        i += 1
        e
      }
    }
  }

  private def genericIterator(
    m: Map[String, String]
  ): java.util.Iterator[java.util.Map.Entry[String, String]] = {
    val it = m.iterator
    new java.util.Iterator[java.util.Map.Entry[String, String]] {

      override def hasNext: Boolean = it.hasNext

      override def next(): java.util.Map.Entry[String, String] = {
        val (k, v) = it.next()
        entry(k, v)
      }
    }
  }
}

object CompressedTagsView {

  /**
    * Create the mapping from original string to compressed form based on the index of
    * common strings. The compressed form is a single character, `idx + 128`. The returned
    * map is unmodifiable as it is shared across the writer threads.
    */
  def compressedStrings(commonStrings: Map[String, Int]): java.util.Map[String, String] = {
    val m = java.util.HashMap.newHashMap[String, String](commonStrings.size)
    commonStrings.foreachEntry { (s, idx) =>
      m.put(s, Character.toString((idx + 128).asInstanceOf[Char]))
    }
    java.util.Collections.unmodifiableMap(m)
  }
}
