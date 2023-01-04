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
package com.netflix.atlas.spring

import com.netflix.atlas.akka.ActorService
import com.netflix.iep.spring.Main
import munit.FunSuite

import scala.util.Using

class CloudWatchConfigurationSuite extends FunSuite {

  test("load module") {
    System.setProperty(
      "netflix.iep.spring.scanPackages",
      "com.netflix.iep.aws2,com.netflix.iep.spring,com.netflix.atlas"
    )
    Using.resource(Main.run(Array.empty)) { m =>
      val service = m.getContext.getBean(classOf[ActorService])
      assert(service.isHealthy)
    }
  }
}
