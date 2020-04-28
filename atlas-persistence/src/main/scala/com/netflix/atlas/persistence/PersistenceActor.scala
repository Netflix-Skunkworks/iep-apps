/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas.persistence

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.histogram.BucketFunctions
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

/**
  * Takes messages from Persistence API handler and store it to s3.
  */
class PersistenceActor(config: Config, registry: Registry) extends Actor with StrictLogging {

  import com.netflix.atlas.webapi.PublishApi._

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private val step = DefaultSettings.stepSize

  // Number of valid datapoints received
  private val numReceivedCounter = BucketCounter.get(
    registry,
    registry.createId("atlas.persistence.numMetricsReceived"),
    BucketFunctions.ageBiasOld(step, TimeUnit.MILLISECONDS)
  )

  def receive: Receive = {
    case req @ PublishRequest(Nil, Nil, _, _) =>
      req.complete(DiagnosticMessage.error(StatusCodes.OK, "empty payload"))
    case req @ PublishRequest(values, Nil, _, _) =>
      process(values)
      req.complete(HttpResponse(StatusCodes.OK))
  }

  private def process(values: List[Datapoint]): Unit = {
    val now = registry.clock().wallTime()
    values.foreach { v =>
      numReceivedCounter.record(now - v.timestamp)
      logger.info(v.toString)
    }
  }
}
