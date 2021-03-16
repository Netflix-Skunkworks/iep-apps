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
package com.netflix.iep.lwc

import com.netflix.spectator.api.Id

class BridgeDatapoint(
  var name: String,
  var tags: Array[String],
  var n: Int,
  val timestamp: Long,
  val value: Double
) {

  def id: Id = Id.unsafeCreate(name, tags, n)

  def tagsMap: java.util.Map[String, String] = {
    val self = this
    new java.util.AbstractMap[String, String] {

      /** Overridden to search the array. */
      override def get(k: AnyRef): String = {
        if (k == "name") {
          self.name
        } else {
          var i = 0
          while (i < self.n) {
            if (k == self.tags(i)) {
              return self.tags(i + 1)
            }
            i += 2
          }
          null
        }
      }

      /** Overridden to search the array. */
      override def containsKey(k: AnyRef): Boolean = get(k) != null

      /**
        * Overridden to just traverse the array.
        */
      override def entrySet(): java.util.Set[java.util.Map.Entry[String, String]] = {
        new java.util.AbstractSet[java.util.Map.Entry[String, String]] {
          override def size(): Int = self.n

          override def iterator(): java.util.Iterator[java.util.Map.Entry[String, String]] = {
            new java.util.Iterator[java.util.Map.Entry[String, String]]
            with java.util.Map.Entry[String, String] {
              private[this] var i = 0

              override def hasNext: Boolean = i < self.n

              override def next(): java.util.Map.Entry[String, String] = {
                i += 2
                this
              }

              override def getKey: String = self.tags(i - 2)

              override def getValue: String = self.tags(i - 1)

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
      if (tags.length >= n + length) {
        System.arraycopy(commonTags, 0, tags, n, length)
        n += length
      } else {
        // expand array if needed for merge
        val tagsArray = new Array[String](n + length)
        System.arraycopy(tags, 0, tagsArray, 0, n)
        System.arraycopy(commonTags, 0, tagsArray, n, length)
        tags = tagsArray
        n += length
      }
    }
  }

  override def toString: String = {
    s"BridgeDatapoint($id, $timestamp, $value)"
  }
}
