/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.iep.lwc.fwd.cw

case class ClusterConfig(
  email: String,
  expressions: List[ForwardingExpression],
  checksToSkip: List[String] = Nil
) {

  def shouldSkip(name: String): Boolean = {
    checksToSkip.contains(name)
  }
}

case class ForwardingExpression(
  atlasUri: String,
  account: String,
  region: Option[String],
  metricName: String,
  dimensions: List[ForwardingDimension] = Nil
) {
  require(atlasUri != null, "atlasUri cannot be null")
  require(account != null, "account cannot be null")
  require(metricName != null, "metricName cannot be null")
}

case class ForwardingDimension(name: String, value: String)
