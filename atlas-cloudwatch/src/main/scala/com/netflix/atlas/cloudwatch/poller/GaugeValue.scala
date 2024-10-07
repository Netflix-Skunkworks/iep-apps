/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch.poller

import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Statistic

case class GaugeValue(tags: Map[String, String], value: Double) {

  def id: Id = {
    import scala.jdk.CollectionConverters._
    val ts = (tags - "name").map {
      case (k, v) => k -> (if (v.length > 120) "VALUE_TOO_LONG" else v)
    }.asJava
    Id.create(tags("name")).withTags(ts).withTag(Statistic.gauge)
  }
}

object GaugeValue {

  def apply(name: String, value: Double): GaugeValue = {
    apply(Map("name" -> name), value)
  }
}
