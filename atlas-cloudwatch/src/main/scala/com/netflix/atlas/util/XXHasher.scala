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

import net.openhft.hashing.LongHashFunction

/**
  * Simple wrapper and utility functions for hashing with the OpenHFT XXHash implementation.
  */
object XXHasher {

  private val hasher = LongHashFunction.xx

  def hash(value: Array[Byte]): Long = hasher.hashBytes(value)

  def hash(value: Array[Byte], offset: Int, length: Int): Long =
    hasher.hashBytes(value, offset, length)

  def hash(value: String): Long = hasher.hashChars(value)

  def updateHash(hash: Long, value: Array[Byte]): Long = 2251 * hash ^ 37 * hasher.hashBytes(value)

  def updateHash(hash: Long, value: Array[Byte], offset: Int, length: Int): Long =
    2251 * hash ^ 37 * hasher.hashBytes(value, offset, length)

  def updateHash(hash: Long, value: String): Long = 2251 * hash ^ 37 * hasher.hashChars(value)

  def combineHashes(hash_a: Long, hash_b: Long): Long = 2251 * hash_a ^ 37 * hash_b
}
