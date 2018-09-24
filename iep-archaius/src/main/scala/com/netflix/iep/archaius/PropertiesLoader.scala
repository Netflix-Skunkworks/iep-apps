/*
 * Copyright 2014-2018 Netflix, Inc.
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
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.netflix.atlas.json.Json
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

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

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  context.system.scheduler.schedule(0.seconds, 5.seconds, self, PropertiesLoader.Tick)

  def receive: Receive = {
    case PropertiesLoader.Tick =>
      val future = dynamoService.execute { client =>
        val matches = List.newBuilder[PropertiesApi.Property]
        val request = new ScanRequest().withTableName(table)
        var response = client.scan(request)
        matches ++= process(response.getItems)
        while (response.getLastEvaluatedKey != null) {
          request.setExclusiveStartKey(response.getLastEvaluatedKey)
          response = client.scan(request)
          matches ++= process(response.getItems)
        }
        matches.result()
      }

      future.onComplete {
        case Success(vs) => propContext.update(vs)
        case Failure(t)  => logger.error("failed to refresh properties from dynamodb", t)
      }
  }

  private def process(items: Items): PropList = {
    import scala.collection.JavaConverters._
    items.asScala
      .filter(_.containsKey("data"))
      .map(_.get("data").getS)
      .map(s => Json.decode[PropertiesApi.Property](s))
      .toList
  }
}

object PropertiesLoader {
  case object Tick
}
