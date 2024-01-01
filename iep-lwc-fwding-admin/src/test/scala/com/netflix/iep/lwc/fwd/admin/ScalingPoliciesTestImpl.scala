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
package com.netflix.iep.lwc.fwd.admin

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext

class ScalingPoliciesTestImpl(
  config: Config,
  dao: ScalingPoliciesDao,
  policies: Map[EddaEndpoint, List[ScalingPolicy]] = Map.empty[EddaEndpoint, List[ScalingPolicy]]
) extends ScalingPolicies(config, dao) {

  scalingPolicies = policies
  override def startPeriodicTimer(): Unit = {}
}

class ScalingPoliciesDaoTestImpl(
  policies: Map[EddaEndpoint, List[ScalingPolicy]]
) extends ScalingPoliciesDao {

  protected implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  override def getScalingPolicies: Flow[EddaEndpoint, List[ScalingPolicy], NotUsed] = {
    Flow[EddaEndpoint]
      .map(policies(_))
  }
}
