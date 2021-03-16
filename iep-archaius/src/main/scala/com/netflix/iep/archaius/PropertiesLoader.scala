/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.iep.archaius

import akka.actor.Actor
import com.netflix.atlas.json.Json
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

import scala.util.Failure
import scala.util.Success

/**
  * Actor for loading properties from dynamodb. Properties will get updated in the provided
  * `PropertiesContext`.
  */
class PropertiesLoader(config: Config, propContext: PropertiesContext, dynamoService: DynamoService)
    extends Actor
    with StrictLogging {

  private val table = config.getString("netflix.iep.archaius.table")

  import scala.jdk.StreamConverters._

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  context.system.scheduler.scheduleAtFixedRate(5.seconds, 5.seconds, self, PropertiesLoader.Tick)

  def receive: Receive = {
    case PropertiesLoader.Tick =>
      val future = dynamoService.execute { client =>
        val request = ScanRequest.builder().tableName(table).build()
        client
          .scanPaginator(request)
          .items()
          .stream()
          .toScala(List)
          .flatMap(process)
      }

      future.onComplete {
        case Success(vs) => propContext.update(vs)
        case Failure(t)  => logger.error("failed to refresh properties from dynamodb", t)
      }
  }

  private def process(item: AttrMap): Option[PropertiesApi.Property] = {
    Option(item.get("data")).map(v => Json.decode[PropertiesApi.Property](v.s()))
  }
}

object PropertiesLoader {
  case object Tick
}
