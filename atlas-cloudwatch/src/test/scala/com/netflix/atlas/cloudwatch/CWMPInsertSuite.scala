/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.assertCWDP
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ce
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.cwv
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.makeFirehoseMetric
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.nts
import com.netflix.atlas.cloudwatch.BaseCloudWatchMetricsProcessorSuite.ts

import scala.concurrent.duration.DurationInt

class CWMPInsertSuite extends BaseCloudWatchMetricsProcessorSuite {

  test("insertDatapoint empty") {
    val updated = processor.insertDatapoint(
      cwDP.toBuilder.clearData().build().toByteArray,
      makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts(-1.minutes)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 1)
    assertCWDP(updated.getData(0), ts(-1.minutes), Array(39.0, 1.0, 7.0, 19))
    assertCounters(appended = 1)
  }

  test("insertDatapoint after") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), ts(-1.minute)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 2)
    assertCWDP(updated.getData(0), ts(-2.minute), Array(39.0, 1.0, 7.0, 19), nts(-1.minute))
    assertCWDP(updated.getData(1), ts(-1.minute), Array(80.0, 2.0, 6.0, 5))
    assertCounters(appended = 1)
  }

  test("insertDatapoint before") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), ts(-3.minute)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 2)
    assertCWDP(updated.getData(0), ts(-3.minute), Array(80.0, 2.0, 6.0, 5))
    assertCWDP(updated.getData(1), ts(-2.minute), Array(39.0, 1.0, 7.0, 19), nts(-1.minutes))
    assertCounters(ooo = 1)
  }

  test("insertDatapoint before published") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-2.minutes, -1.minutes, true, Some(Array(39.0, 1.0, 7.0, 19)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(80.0, 2.0, 6.0, 5), ts(-3.minute)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 2)
    assertCWDP(updated.getData(0), ts(-3.minute), Array(80.0, 2.0, 6.0, 5))
    assertCWDP(updated.getData(1), ts(-2.minute), Array(39.0, 1.0, 7.0, 19), nts(-1.minutes))
    assertCounters(beforePublished = 1)
  }

  test("insertDatapoint between") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-4.minutes, -3.minutes, false, Some(Array(80.0, 2.0, 6.0, 5))),
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(2.0, 0.0, 1.0, 2), ts(-3.minutes)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), ts(-4.minutes), Array(80.0, 2.0, 6.0, 5), nts(-3.minutes))
    assertCWDP(updated.getData(1), ts(-3.minutes), Array(2.0, 0.0, 1.0, 2))
    assertCWDP(updated.getData(2), ts(-2.minutes), Array(39.0, 1.0, 7.0, 19), nts(-1.minute))
    assertCounters(ooo = 1)
  }

  test("insertDatapoint duplicate same value") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(39.0, 1.0, 7.0, 19), ts(-2.minutes)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 1)
    assertCWDP(updated.getData(0), ts(-2.minutes), Array(39.0, 1.0, 7.0, 19), nts(-1.minute))
    assertCounters(dupes = 1)
  }

  test("insertDatapoint update oldest entry") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-3.minutes, -2.minutes, false, Some(Array(2.0, 0.0, 1.0, 2))),
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19))),
          cwv(-1.minutes, 0.minutes, false, Some(Array(80.0, 2.0, 6.0, 5)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(-1.0, -1.0, -1.0, -2), ts(-3.minutes)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), ts(-3.minutes), Array(-1.0, -1.0, -1.0, -2))
    assertCWDP(updated.getData(1), ts(-2.minutes), Array(39.0, 1.0, 7.0, 19), nts(-1.minutes))
    assertCWDP(updated.getData(2), ts(-1.minutes), Array(80.0, 2.0, 6.0, 5), nts)
    assertCounters(updates = 1)
  }

  test("insertDatapoint same middle") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-3.minutes, -2.minutes, false, Some(Array(2.0, 0.0, 1.0, 2))),
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19))),
          cwv(-1.minutes, 0.minutes, false, Some(Array(80.0, 2.0, 6.0, 5)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(-1.0, -1.0, -1.0, -2), ts(-2.minute)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), ts(-3.minutes), Array(2.0, 0.0, 1.0, 2), nts(-2.minutes))
    assertCWDP(updated.getData(1), ts(-2.minutes), Array(-1.0, -1.0, -1.0, -2))
    assertCWDP(updated.getData(2), ts(-1.minute), Array(80.0, 2.0, 6.0, 5))
    assertCounters(updates = 1)
  }

  test("insertDatapoint same last") {
    val updated = processor.insertDatapoint(
      ce(
        List(
          cwv(-3.minutes, -2.minutes, false, Some(Array(2.0, 0.0, 1.0, 2))),
          cwv(-2.minutes, -1.minutes, false, Some(Array(39.0, 1.0, 7.0, 19))),
          cwv(-1.minutes, 0.minutes, false, Some(Array(80.0, 2.0, 6.0, 5)))
        )
      ).toByteArray,
      makeFirehoseMetric(Array(-1.0, -1.0, -1.0, -2), ts(-1.minute)),
      category,
      nts
    )
    assertEquals(updated.getDataCount, 3)
    assertCWDP(updated.getData(0), ts(-3.minutes), Array(2.0, 0.0, 1.0, 2), nts(-2.minutes))
    assertCWDP(updated.getData(1), ts(-2.minutes), Array(39.0, 1.0, 7.0, 19), nts(-1.minute))
    assertCWDP(updated.getData(2), ts(-1.minute), Array(-1.0, -1.0, -1.0, -2))
    assertCounters(updates = 1)
  }

  test("insertDatapoint evict") {
    var updated: CloudWatchCacheEntry = null
    for (i <- 15 until 2 by -1) {
      updated = processor.insertDatapoint(
        if (updated == null) {
          cwDP.toBuilder.clearData().build().toByteArray
        } else {
          updated.toByteArray
        },
        makeFirehoseMetric(Array(i, i, i, i), ts(-i.minutes)),
        category,
        nts(-i.minutes)
      )
    }
    assertEquals(updated.getDataCount, 6)
    var idx = 0
    for (i <- 8 until 2 by -1) {
      assertCWDP(updated.getData(idx), ts(-i.minutes), Array(i, i, i, i), nts(-i.minutes))
      idx += 1
    }
    assertCounters(appended = 13)
  }

  def assertCounters(
    updates: Long = 0,
    dupes: Long = 0,
    ooo: Long = 0,
    beforePublished: Long = 0,
    inserted: Long = 0,
    appended: Long = 0
  ): Unit = {
    assertEquals(
      registry
        .counter(
          processor.insert.withTags(
            "aws.namespace",
            category.namespace,
            "aws.metric",
            "SumRate",
            "state",
            "updated"
          )
        )
        .count(),
      updates
    )
    assertEquals(
      registry
        .counter(
          processor.insert.withTags(
            "aws.namespace",
            category.namespace,
            "aws.metric",
            "SumRate",
            "state",
            "duplicates"
          )
        )
        .count(),
      dupes
    )
    assertEquals(
      registry
        .counter(
          processor.insert
            .withTags("aws.namespace", category.namespace, "aws.metric", "SumRate", "state", "ooo")
        )
        .count(),
      ooo
    )
    assertEquals(
      registry
        .counter(
          processor.insert.withTags(
            "aws.namespace",
            category.namespace,
            "aws.metric",
            "SumRate",
            "state",
            "beforePublished"
          )
        )
        .count(),
      beforePublished
    )
    assertEquals(
      registry
        .counter(
          processor.insert.withTags(
            "aws.namespace",
            category.namespace,
            "aws.metric",
            "SumRate",
            "state",
            "inserted"
          )
        )
        .count(),
      inserted
    )
    assertEquals(
      registry
        .counter(
          processor.insert.withTags(
            "aws.namespace",
            category.namespace,
            "aws.metric",
            "SumRate",
            "state",
            "appended"
          )
        )
        .count(),
      appended
    )
  }

}
