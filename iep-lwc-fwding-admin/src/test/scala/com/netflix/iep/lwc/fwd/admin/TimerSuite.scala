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
package com.netflix.iep.lwc.fwd.admin

import com.netflix.iep.lwc.fwd.admin.Timer._
import com.netflix.spectator.api.ManualClock
import munit.FunSuite

class TimerSuite extends FunSuite {

  test("Measure time for calls that succeed") {
    val clock = new ManualClock()
    clock.setMonotonicTime(1)

    def work(): String = {
      clock.setMonotonicTime(2)
      "done"
    }

    val timer = new TestTimer()

    val result = measure(work(), "workTimer", clock, timer.record)
    assert(result == "done")

    assert(timer.name == "workTimer")
    assert(timer.tags.isEmpty)
    assert(timer.duration == 1)
  }

  test("Measure time for calls that fail") {
    val clock = new ManualClock()
    clock.setMonotonicTime(1)

    def work(): String = {
      clock.setMonotonicTime(2)
      throw new RuntimeException("failed")
    }

    val timer = new TestTimer()

    intercept[RuntimeException](
      measure(work(), "workTimer", clock, timer.record)
    )
    assert(timer.name == "workTimer")
    assertEquals(timer.tags, List("exception", "RuntimeException"))
    assert(timer.duration == 1)

  }
}

class TestTimer(var name: String = "", var tags: List[String] = Nil, var duration: Long = -1) {

  def record(name: String, tags: List[String], duration: Long): Unit = {
    this.name = name
    this.tags = tags
    this.duration = duration
  }
}
