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

  val ExpressionId = "ExpressionId"
  val LastReportTs = "LastReportTs"
  val NoDataAgeMins = "NoDataAgeMins"
  val NoScalingPolicyAgeMins = "NoScalingPolicyAgeMins"
  val FwdMetricInfo = "FwdMetricInfo"
  val Error = "Error"
  val ScalingPolicy = "ScalingPolicy"

  private val table = new DynamoDB(dynamoDBClient)
    .getTable("iep.lwc.fwd.cw.ExpressionDetails")

  override def save(exprDetails: ExpressionDetails): Unit = {
    var item =
      new Item()
        .withPrimaryKey(ExpressionId, Json.encode(exprDetails.expressionId))
        .withNumber(LastReportTs, exprDetails.lastReportTs)
        .withNumber(NoDataAgeMins, exprDetails.noDataAgeMins)
        .withNumber(NoScalingPolicyAgeMins, exprDetails.noScalingPolicyMins)

    exprDetails.fwdMetricInfo.map { m =>
      item = item.withJSON(FwdMetricInfo, Json.encode(m))
    }

    exprDetails.error.map { e =>
      item = item.withJSON(Error, Json.encode(e))
    }

    exprDetails.scalingPolicy.map { s =>
      item = item.withJSON(ScalingPolicy, Json.encode(s))
    }

    table.putItem(item)
  }

  override def read(id: ExpressionId): Option[ExpressionDetails] = {
    val item = table.getItem(new PrimaryKey(ExpressionId, Json.encode(id)))
    Option(item).map { i =>
      new ExpressionDetails(
        id,
        i.getNumber(LastReportTs).longValue(),
        Option(i.getJSON(FwdMetricInfo)).map(Json.decode[FwdMetricInfo](_)),
        Option(i.getJSON(Error)).map(Json.decode[Throwable](_)),
        i.getNumber(NoDataAgeMins).intValue(),
        i.getNumber(NoScalingPolicyAgeMins).intValue(),
        Option(i.getJSON(ScalingPolicy)).map(Json.decode[JsonNode](_))
      )
    }
  }

}

case class ExpressionDetails(
  expressionId: ExpressionId,
  lastReportTs: Long,
  fwdMetricInfo: Option[FwdMetricInfo],
  error: Option[Throwable],
  noDataAgeMins: Int,
  noScalingPolicyMins: Int,
  scalingPolicy: Option[JsonNode]
)

case class VersionMismatchException() extends Exception
