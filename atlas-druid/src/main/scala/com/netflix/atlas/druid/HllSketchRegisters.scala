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
package com.netflix.atlas.druid

import com.netflix.spectator.api.patterns.DistinctCountSketch

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Base64

/**
  * Extracts the per-register values from an Apache DataSketches HLL sketch so that a Druid
  * native sketch column can be exposed using the same representation as a Spectator
  * `DistinctCountSketch`, that is one max-gauge per register tagged with `distinct=R##`.
  *
  * Druid returns the serialized sketch, base64 encoded, when the `HLLSketchMerge` aggregation
  * is used with `shouldFinalize` disabled. Requesting `lgK=6` makes Druid down-sample the
  * stored sketch to the 64 registers used by `DistinctCountSketch` as part of the merge, so
  * the registers extracted here can be merged, across both space and time, with registers
  * coming from any other source by taking the max.
  *
  * The serialization format is documented with the DataSketches library. The library itself is
  * not used because it does not expose the register values through its public API.
  */
object HllSketchRegisters {

  /** Number of registers expected by the backend estimator. */
  val registers: Int = DistinctCountSketch.REGISTERS

  /** Number of low bits of the coupon used to select the register. */
  private val indexBits: Int = Integer.numberOfTrailingZeros(registers)

  // Layout of the preamble that is common to all modes.
  private val lgKOffset = 3
  private val lgCouponArrOffset = 4
  private val flagsOffset = 5
  private val listCountOffset = 6
  private val modeOffset = 7

  private val emptyFlag = 4
  private val compactFlag = 8

  // Modes, from the low two bits of the mode byte. The next two bits hold the target HLL type.
  private val listMode = 0
  private val setMode = 1
  private val hllMode = 2

  // Coupons are packed into an int as (rho << 26) | slot.
  private val couponKeyBits = 26

  // Offset of the coupon array for each of the sparse modes, which is the size of the
  // preamble for that mode. The set mode preamble also holds the coupon count.
  private val listCouponOffset = 8
  private val setCountOffset = 8
  private val setCouponOffset = 12

  // Dense (HLL) mode preamble. The register array follows it.
  private val hllCurMinOffset = 6
  private val hllAuxCountOffset = 36
  private val hllRegisterOffset = 40

  /**
    * Decode the base64 encoded sketch returned by Druid into the per-register max rho values.
    * Registers that were never set are left as zero, matching how an unset register is treated
    * by the estimator.
    *
    * @param base64Sketch
    *     Base64 encoded, serialized DataSketches HLL sketch.
    * @return
    *     Array of `registers` rho values.
    */
  def decode(base64Sketch: String): Array[Int] = {
    decode(Base64.getDecoder.decode(base64Sketch))
  }

  /** Decode the serialized sketch bytes into the per-register max rho values. */
  def decode(sketch: Array[Byte]): Array[Int] = {
    require(sketch.length > modeOffset, s"sketch is truncated: ${sketch.length} bytes")
    val buffer = ByteBuffer.wrap(sketch).order(ByteOrder.LITTLE_ENDIAN)
    val values = new Array[Int](registers)
    if ((sketch(flagsOffset) & emptyFlag) == 0) {
      val compact = (sketch(flagsOffset) & compactFlag) != 0
      sketch(modeOffset) & 0x3 match {
        case `listMode` =>
          val count = sketch(listCountOffset) & 0xFF
          decodeCoupons(buffer, listCouponOffset, count, compact, values)
        case `setMode` =>
          val count = buffer.getInt(setCountOffset)
          decodeCoupons(buffer, setCouponOffset, count, compact, values)
        case `hllMode` => decodeHll(buffer, values)
        case m         => throw new IllegalArgumentException(s"unsupported HLL sketch mode: $m")
      }
    }
    values
  }

  /**
    * Sparse modes keep the raw coupons rather than a register array. A coupon packs the rho
    * value with the full slot address, so the register is the low bits of that address, the
    * same split that would be applied when the sketch is promoted to the dense mode.
    *
    * The compact serialization writes only the coupons that are set, so there are `count` of
    * them. The updatable serialization instead writes the whole slot array, with empty slots
    * left as zero, so all of the slots have to be scanned to find the coupons.
    */
  private def decodeCoupons(
    buffer: ByteBuffer,
    offset: Int,
    count: Int,
    compact: Boolean,
    values: Array[Int]
  ): Unit = {
    val slots = if (compact) count else 1 << buffer.get(lgCouponArrOffset)
    // Never read past the end of the payload, whatever the preamble claims.
    val length = math.min(slots, (buffer.capacity() - offset) / 4)
    var i = 0
    while (i < length) {
      val coupon = buffer.getInt(offset + 4 * i)
      // Empty slots of the updatable form are zero and should be ignored.
      if (coupon != 0) {
        val idx = coupon & (registers - 1)
        val rho = coupon >>> couponKeyBits
        if (rho > values(idx)) values(idx) = rho
      }
      i += 1
    }
  }

  /**
    * Dense mode keeps a packed register array. For the HLL_4 type each register is a nibble
    * holding the offset from `curMin`, with the maximum nibble value indicating that the actual
    * value is kept in an auxiliary hash map. For HLL_6 and HLL_8 the value is stored directly.
    */
  private def decodeHll(buffer: ByteBuffer, values: Array[Int]): Unit = {
    val lgK = buffer.get(lgKOffset)
    require(
      1 << lgK == registers,
      s"sketch has ${1 << lgK} registers, expected $registers, query must specify lgK=$indexBits"
    )
    (buffer.get(modeOffset) >>> 2) & 0x3 match {
      case 0 => decodeHll4(buffer, values)
      case 1 => decodeHll6(buffer, values)
      case 2 => decodeHll8(buffer, values)
      case t => throw new IllegalArgumentException(s"unsupported target HLL type: $t")
    }
  }

  private def decodeHll4(buffer: ByteBuffer, values: Array[Int]): Unit = {
    val curMin = buffer.get(hllCurMinOffset) & 0xFF
    var i = 0
    while (i < registers) {
      val packed = buffer.get(hllRegisterOffset + i / 2) & 0xFF
      val nibble = if (i % 2 == 0) packed & 0xF else packed >>> 4
      values(i) = nibble + curMin
      i += 1
    }
    decodeAux(buffer, values)
  }

  /**
    * Registers with a value too large for the nibble encoding are kept in an auxiliary hash map
    * that follows the register array. Each entry is a coupon holding the register index and its
    * actual value, so it simply replaces what was read from the nibble.
    */
  private def decodeAux(buffer: ByteBuffer, values: Array[Int]): Unit = {
    val auxCount = buffer.getInt(hllAuxCountOffset)
    if (auxCount > 0) {
      val offset = hllRegisterOffset + registers / 2
      var i = 0
      while (i < auxCount) {
        val coupon = buffer.getInt(offset + 4 * i)
        if (coupon != 0) {
          values(coupon & (registers - 1)) = coupon >>> couponKeyBits
        }
        i += 1
      }
    }
  }

  private def decodeHll6(buffer: ByteBuffer, values: Array[Int]): Unit = {
    var i = 0
    while (i < registers) {
      val bitIdx = i * 6
      val byteIdx = hllRegisterOffset + bitIdx / 8
      val shift = bitIdx % 8
      val chunk = (buffer.get(byteIdx) & 0xFF) | ((buffer.get(byteIdx + 1) & 0xFF) << 8)
      values(i) = (chunk >>> shift) & 0x3F
      i += 1
    }
  }

  private def decodeHll8(buffer: ByteBuffer, values: Array[Int]): Unit = {
    var i = 0
    while (i < registers) {
      values(i) = buffer.get(hllRegisterOffset + i) & 0xFF
      i += 1
    }
  }

  /** Precomputed register tag values, matching those used by `DistinctCountSketch`. */
  val tagValues: Array[String] =
    Array.tabulate(registers)(i => "R%02X".format(i))
}
