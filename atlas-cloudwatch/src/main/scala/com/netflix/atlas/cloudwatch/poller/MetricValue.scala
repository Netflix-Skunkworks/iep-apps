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
package com.netflix.atlas.cloudwatch.poller

import com.netflix.spectator.api.Id

sealed trait MetricValue {
  def id: Id
}

object MetricValue {

  // Utility function to create an Id with tags
  def createId(name: String, tags: Map[String, String]): Id = {
    import scala.jdk.CollectionConverters._
    val ts = (tags - "name").map {
      case (k, v) => k -> (if (v.length > 120) "VALUE_TOO_LONG" else v)
    }.asJava
    Id.create(name).withTags(ts)
  }

  def apply(name: String, value: Double): MetricValue = {
    DoubleValue(Map("name" -> name), value)
  }
}

// Case class for Double values
case class DoubleValue(tags: Map[String, String], value: Double) extends MetricValue {
  def id: Id = MetricValue.createId(tags("name"), tags)
}
