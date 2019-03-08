/*
 * Copyright 2014-2019 Netflix, Inc.
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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw._
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

trait ExpressionDetailsDao {
  def save(exprDetails: ExpressionDetails): Unit
  def read(id: ExpressionId): Option[ExpressionDetails]
}

class ExpressionDetailsDaoImpl @Inject()(dynamoDBClient: AmazonDynamoDB)
    extends ExpressionDetailsDao
    with StrictLogging {
  import scala.collection.JavaConverters._
  import ExpressionDetails._

  private val table = new DynamoDB(dynamoDBClient)
    .getTable("iep.lwc.fwd.cw.ExpressionDetails")

  override def save(exprDetails: ExpressionDetails): Unit = {
    var item =
      new Item()
        .withPrimaryKey(ExpressionIdAttr, Json.encode(exprDetails.expressionId))
        .withNumber(Timestamp, exprDetails.timestamp)
        .withMap(Events, exprDetails.events.asJava)

    exprDetails.fwdMetricInfo.map { m =>
      item = item.withString(FwdMetricInfoAttr, Json.encode(m))
    }

    exprDetails.error.map { e =>
      item = item.withString(Error, Json.encode(e))
    }

    exprDetails.scalingPolicy.map { s =>
      item = item.withString(ScalingPolicy, Json.encode(s))
    }

    table.putItem(item)
  }

  override def read(id: ExpressionId): Option[ExpressionDetails] = {
    val item = table.getItem(new PrimaryKey(ExpressionIdAttr, Json.encode(id)))
    Option(item).map { i =>
      new ExpressionDetails(
        id,
        i.getNumber(Timestamp).longValue(),
        Option(i.getString(FwdMetricInfoAttr)).map(Json.decode[FwdMetricInfo](_)),
        Option(i.getString(Error)).map(Json.decode[Throwable](_)),
        i.getMap[Long](Events).asScala.toMap,
        Option(i.getString(ScalingPolicy)).map(Json.decode[JsonNode](_))
      )
    }
  }

}

case class ExpressionDetails(
  expressionId: ExpressionId,
  timestamp: Long,
  fwdMetricInfo: Option[FwdMetricInfo],
  error: Option[Throwable],
  events: Map[String, Long],
  scalingPolicy: Option[JsonNode]
)

object ExpressionDetails {
  val ExpressionIdAttr = "ExpressionId"
  val Timestamp = "timestamp"
  val FwdMetricInfoAttr = "FwdMetricInfo"
  val Error = "Error"
  val Events = "events"
  val DataFoundEvent = "dataFoundEvent"
  val ScalingPolicyFoundEvent = "scalingPolicyFoundEvent"
  val ScalingPolicy = "ScalingPolicy"
}

case class VersionMismatchException() extends Exception
