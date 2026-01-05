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

import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class AggrConfigSuite extends FunSuite {

  test("initial polling delay") {
    val step = 60000L
    val clock = new ManualClock()
    val config = new AggrConfig(ConfigFactory.load(), new NoopRegistry, null)
    (0L until step).foreach { t =>
      clock.setWallTime(t)
      val delay = config.initialPollingDelay(clock, step)
      val stepOffset = (t + delay) % step
      assert(stepOffset >= 3000)
      assert(stepOffset <= 48000)
    }
  }
}
