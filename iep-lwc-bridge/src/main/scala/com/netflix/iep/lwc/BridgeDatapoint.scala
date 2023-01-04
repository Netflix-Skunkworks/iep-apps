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
package com.netflix.iep.lwc

import com.netflix.spectator.api.Id

class BridgeDatapoint(
  var name: String,
  var tags: Array[String],
  var n: Int,
  val timestamp: Long,
  val value: Double
) {

  private var cachedId: Id = _

  def id: Id = {
    if (cachedId == null) {
      cachedId = Id.unsafeCreate(name, tags, n)
    }
    cachedId
  }

  def tagsMap: java.util.Map[String, String] = {
    val tagSet = id
    new java.util.AbstractMap[String, String] {

      /** Overridden to search the array. */
      override def get(k: AnyRef): String = {
        var i = 0
        val size = tagSet.size()
        while (i < size) {
          if (k == tagSet.getKey(i)) {
            return tagSet.getValue(i)
          }
          i += 1
        }
        null
      }

      /** Overridden to search the array. */
      override def containsKey(k: AnyRef): Boolean = get(k) != null

      /**
        * Overridden to just traverse the array.
        */
      override def entrySet(): java.util.Set[java.util.Map.Entry[String, String]] = {
        new java.util.AbstractSet[java.util.Map.Entry[String, String]] {
          override def size(): Int = tagSet.size()

          override def iterator(): java.util.Iterator[java.util.Map.Entry[String, String]] = {
            new java.util.Iterator[java.util.Map.Entry[String, String]]
              with java.util.Map.Entry[String, String] {
              private[this] var i = -1

              override def hasNext: Boolean = i < tagSet.size() - 1

              override def next(): java.util.Map.Entry[String, String] = {
                i += 1
                this
              }

              override def getKey: String = tagSet.getKey(i)

              override def getValue: String = tagSet.getValue(i)

              override def setValue(value: String): String = {
                throw new UnsupportedOperationException("setValue")
              }
            }
          }
        }
      }
    }
  }

  def merge(commonName: String, commonTags: Array[String], length: Int): Unit = {
    if (name == null)
      name = commonName
    if (length > 0) {
      // Common tags must come first for deduping to give precedence to the tags on the
      // local metric.
      val tagsArray = new Array[String](n + length)
      System.arraycopy(commonTags, 0, tagsArray, 0, length)
      System.arraycopy(tags, 0, tagsArray, length, n)
      tags = tagsArray
      n += length

      // Force it to be recomputed if there is a change
      cachedId = null
    }
  }

  override def toString: String = {
    s"BridgeDatapoint($id, $timestamp, $value)"
  }
}
