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
import munit.FunSuite

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Base64

/**
  * The sketches used here were serialized by the Apache DataSketches library, either directly
  * or by a druid `HLLSketchMerge` aggregation with `shouldFinalize` disabled, so the decoded
  * registers are checked against values written by that library rather than against something
  * this code produced.
  */
class HllSketchRegistersSuite extends FunSuite {

  import HllSketchRegistersSuite.*

  /**
    * A dense sketch carries `KxQ`, the sum of `2^-register` over all of its registers, in the
    * preamble along with the number of registers still at the minimum value. Recomputing both
    * from the decoded registers checks every register against the library.
    */
  private def assertMatchesPreamble(base64Sketch: String): Unit = {
    val sketch = Base64.getDecoder.decode(base64Sketch)
    val buffer = ByteBuffer.wrap(sketch).order(ByteOrder.LITTLE_ENDIAN)
    val values = HllSketchRegisters.decode(sketch)

    // KxQ is maintained incrementally by the library, so it is not required to be bit for bit
    // identical to a fresh summation over the decoded registers.
    val kxq = values.foldLeft(0.0)((acc, v) => acc + math.pow(2.0, -v))
    assertEqualsDouble(kxq, buffer.getDouble(16) + buffer.getDouble(24), 1e-9)

    // For HLL_4 the count is of registers at the minimum value tracked in the preamble, for
    // the other types it is a count of the registers that are still unset.
    val hll4 = ((buffer.get(7) >>> 2) & 0x3) == 0
    val curMin = if (hll4) buffer.get(6) & 0xFF else 0
    assertEquals(values.count(_ == curMin), buffer.getInt(32))
  }

  private def assertCardinality(base64Sketch: String, expected: Double): Unit = {
    val values = HllSketchRegisters.decode(base64Sketch)
    val actual = DistinctCountSketch.cardinality(values.map(_.toDouble))
    // The estimator here is not the one used by DataSketches, which applies a composite
    // estimator with an interpolation table for bias correction. Both are approximating the
    // same register array, so they are only required to agree within the ~13% standard error
    // that the register count allows.
    val diff = math.abs(actual - expected) / math.max(expected, 1.0)
    assert(diff < 0.13, s"expected ~$expected, got $actual")
  }

  test("empty sketch has no registers set") {
    (hll4Empty :: hll6Empty :: hll8Empty :: Nil).foreach { sketch =>
      val values = HllSketchRegisters.decode(sketch)
      assertEquals(values.length, DistinctCountSketch.REGISTERS)
      assert(values.forall(_ == 0))
    }
  }

  test("list mode, three values") {
    (hll4List3 :: hll6List3 :: hll8List3 :: Nil).foreach { sketch =>
      val values = HllSketchRegisters.decode(sketch)
      // Three values recorded, so at most three registers can be set.
      assertEquals(values.count(_ > 0), 3)
      assertCardinality(sketch, 3.0)
    }
  }

  test("list mode, merged sketches") {
    assertCardinality(mergedList6, 6.0)
    assertCardinality(mergedList3, 3.0)
  }

  test("registers match the preamble written by the library") {
    denseSketches.foreach(assertMatchesPreamble)
  }

  test("estimate is within the error bound of the library estimate") {
    assertCardinality(hll4Dense100, 89.57661129014312)
    assertCardinality(hll6Dense100, 89.57661129014312)
    assertCardinality(hll8Dense100, 89.57661129014312)

    assertCardinality(hll4Dense100k, 87294.42087371077)
    assertCardinality(hll6Dense100k, 87294.42087371077)
    assertCardinality(hll8Dense100k, 87294.42087371077)

    assertCardinality(hll4Dense50m, 4.992581800108275e7)
    assertCardinality(hll6Dense50m, 4.992581800108275e7)
    assertCardinality(hll8Dense50m, 4.992581800108275e7)
  }

  test("HLL_4 auxiliary hash map") {
    // Registers whose value exceeds curMin+14 do not fit the nibble encoding and are kept in an
    // auxiliary hash map after the register array. They are also the largest rho values, so they
    // dominate the estimate. Expected values come from the library's own register iterator.
    val values = HllSketchRegisters.decode(hll4Aux)
    assertEquals(values.toList, hll4AuxRegisters)
    assertMatchesPreamble(hll4Aux)
  }

  test("all target types give the same registers") {
    val hll4 = HllSketchRegisters.decode(hll4Dense100k).toList
    assertEquals(HllSketchRegisters.decode(hll6Dense100k).toList, hll4)
    assertEquals(HllSketchRegisters.decode(hll8Dense100k).toList, hll4)
  }

  test("HLL mode, merged sketches") {
    // Rescaled down from lgK=12 by the merge, with the estimate the library reported for the
    // same sketch.
    assertCardinality(mergedDense200k, 200543.44317154953)
    assertCardinality(mergedDense600k, 713591.4629026129)
    assertCardinality(mergedDense1500k, 1652769.592150117)
    List(mergedDense200k, mergedDense600k, mergedDense1500k).foreach(assertMatchesPreamble)
  }

  test("registers merge across sketches by taking the max") {
    val a = HllSketchRegisters.decode(mergedDense200k).map(_.toDouble)
    val b = HllSketchRegisters.decode(mergedDense1500k).map(_.toDouble)
    val merged = a.indices.map(i => math.max(a(i), b(i))).toArray
    val estimate = DistinctCountSketch.cardinality(merged)
    // A union has to be at least as large as either of the inputs.
    assert(estimate >= DistinctCountSketch.cardinality(a))
    assert(estimate >= DistinctCountSketch.cardinality(b))
  }

  test("register tag values match the spectator encoding") {
    assertEquals(HllSketchRegisters.tagValues.length, DistinctCountSketch.REGISTERS)
    assertEquals(HllSketchRegisters.tagValues.head, "R00")
    assertEquals(HllSketchRegisters.tagValues(10), "R0A")
    assertEquals(HllSketchRegisters.tagValues.last, "R3F")
  }

  test("register count mismatch is rejected") {
    // The same sketch with lgK changed from 6 to 12, which is what would come back if the query
    // failed to ask druid to rescale to the register count used here.
    val sketch = Base64.getDecoder.decode(hll4Dense100k)
    sketch(3) = 12
    val e = intercept[IllegalArgumentException] {
      HllSketchRegisters.decode(sketch)
    }
    assert(e.getMessage.contains("lgK=6"), e.getMessage)
  }
}

object HllSketchRegistersSuite {

  // Sketches serialized directly by the DataSketches library, lgK=6, for a range of
  // cardinalities and each of the three target types.

  val hll4Empty = "AgEHBgMMAAA="
  val hll6Empty = "AgEHBgMMAAQ="
  val hll8Empty = "AgEHBgMMAAg="

  val hll4List3 = "AgEHBgMIAwDL18IEK/L7BoYv+Q0="
  val hll6List3 = "AgEHBgMIAwTL18IEK/L7BoYv+Q0="
  val hll8List3 = "AgEHBgMIAwjL18IEK/L7BoYv+Q0="

  val hll4Dense100 =
    "CgEHBgAIAALSagoz52RWQAAAAAAAvDxAAAAAAAAAAAAPAAAAAAAAABBxIRMBECAXEBFBYDAhMUYkIUFQQBUR" +
      "RAQDMwEDIAIR"

  val hll6Dense100 =
    "CgEHBgAAAAbSagoz52RWQAAAAAAAvDxAAAAAAAAAAAAPAAAAAAAAAEAQHIEwBAEABIBwBEAQBAEBGMAQCMFg" +
      "EIQQCAEBFABRBEFAEAQwAMMQAAMACAIQBAA="

  val hll8Dense100 =
    "CgEHBgAAAArSagoz52RWQAAAAAAAvDxAAAAAAAAAAAAPAAAAAAAAAAABAQcBAgMBAQAAAQACBwEAAQEBAQQA" +
      "BgADAQIBAwYEBAIBAgEEAAUABAUBAQEEBAQAAwADAwEAAwAAAgIAAQE="

  val hll4Dense100k =
    "CgEHBgAICQJ4Eua75k/1QAAAAAAApZ8/AAAAAAAAAAAEAAAAAAAAAGMxUxJCIiNUUzUWEBBFkiNUYTJDJDMm" +
      "dEEiEhQiAhAx"

  val hll6Dense100k =
    "CgEHBgAAAAZ4Eua75k/1QAAAAAAApZ8/AAAAAAAAAAAAAAAAAAAAAMyjMIyzKEuzLMzSOIzjMI+SKIniNIvE" +
      "LI2jPAvDNM3CMM/SQEqzLIvSKMuyJImiMAA="

  val hll8Dense100k =
    "CgEHBgAAAAp4Eua75k/1QAAAAAAApZ8/AAAAAAAAAAAAAAAAAAAAAAwPCgwMDgsKCw0LCwwLDQ4MDg4MDwoJ" +
      "CgkKDg0LEgwLDQ4KDwsMDA0NCwwMDwsNEAoNCwsLCg0KCwsLCQkKCgw="

  val hll4Dense50m =
    "CgEHBgAIEgKsNwLQdc6HQQAAAAAANhE/AAAAAAAAAAAFAAAAAAAAADNTIQUkYmJBMTICEjMkOCRBATQRJEIy" +
      "JCJiMSIlgTIA"

  val hll6Dense50m =
    "CgEHBgAAAAasNwLQdc6HQQAAAAAANhE/AAAAAAAAAAAAAAAAAAAAAFVVXRN1SRZFYRQ2WVNFVZRETVVlUVpl" +
      "UZM1SVY1TRZFWVRlURRFYVNFURc1aVQlSQA="

  val hll8Dense50m =
    "CgEHBgAAAAqsNwLQdc6HQQAAAAAANhE/AAAAAAAAAAAAAAAAAAAAABUVFRcTFBcSFhQUGBQYExYTFRQVFBIU" +
      "ExUVFhQaFRYUExYTEhYVExMWFBQWFBUWFBQUFBgTFRQUFxQTGhQVEhI="

  // Sketches produced by an HllUnion over lgK=12 inputs, down to lgK=6. This is the shape a
  // druid HLLSketchMerge aggregation returns with shouldFinalize disabled, including rescaling
  // the stored sketch as part of the merge and the resulting compact, out of order flags.

  val mergedList3 = "AgEHBgMIAwDL18IEjcSJCefPlRc="
  val mergedList6 = "AgEHBgMIBgDL18IE58+VF30jPAqeLWoHb8tKB/XpZg0="

  val mergedDense200k =
    "CgEHBgAYCgIAAAAAAAAAAAAAAACAqo0/AAAAAAAAAAACAAAAAAAAADJyVSE1ISJSEkEgI7QhJChRMSMhcxK1" +
      "IzAlIlQkQSFD"

  val mergedDense600k =
    "CgEHBgAYCwIAAAAAAAAAAAAAAACgrHA/AAAAAAAAAAABAAAAAAAAACI2RWxENFFFcVJUMioxRjdjUjQ1Y3Q0" +
      "AjNDVUQjIiRk"

  val mergedDense1500k =
    "CgEHBgAYDQIAAAAAAAAAAAAAAAAAzFw/AAAAAAAAAAACAAAAAAAAADNCI0IyEiEhNRFUEoVzMQUmVDIzQVeC" +
      "RSEiNSEiRWEQ"

  // 100k ordinary updates, plus three registers forced well above curMin+14 so that they spill
  // into the HLL_4 auxiliary hash map. The expected registers are what the library's own
  // PairIterator reports for this sketch.

  val hll4Aux =
    "CgEHBgIICQLCDL5cidb2QAAAAAAAVZ0/AAAAAAIg4D0DAAAAAwAAAGPxUxJCIiNU8zUWEBBFkiNUYTJDJDMmdEEi" +
      "EhQiAh8xPAAAhBEAANADAACg"

  val hll4AuxRegisters = List(
    12, 15, 10, 40, 12, 14, 11, 10, 11, 13, 11, 11, 12, 11, 13, 14, 12, 52, 14, 12, 15, 10, 9, 10,
    9, 10, 14, 13, 11, 18, 12, 11, 13, 14, 10, 15, 11, 12, 12, 13, 13, 11, 12, 12, 15, 11, 13, 16,
    10, 13, 11, 11, 11, 10, 13, 10, 11, 11, 11, 9, 33, 10, 10, 12
  )

  val denseSketches = List(
    hll4Dense100,
    hll6Dense100,
    hll8Dense100,
    hll4Dense100k,
    hll6Dense100k,
    hll8Dense100k,
    hll4Dense50m,
    hll6Dense50m,
    hll8Dense50m
  )
}
