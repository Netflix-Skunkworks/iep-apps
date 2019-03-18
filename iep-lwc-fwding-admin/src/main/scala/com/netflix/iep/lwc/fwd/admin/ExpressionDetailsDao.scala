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

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw._
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

trait ExpressionDetailsDao {
  def save(exprDetails: ExpressionDetails): Unit
  def read(id: ExpressionId): Option[ExpressionDetails]
  def scan(): List[ExpressionId]
  def queryPurgeEligible(now: Long, events: List[String]): List[ExpressionId]
  def delete(id: ExpressionId): Unit
  def isPurgeEligible(ed: ExpressionDetails, now: Long): Boolean
}

class ExpressionDetailsDaoImpl @Inject()(
  config: Config,
  dynamoDBClient: AmazonDynamoDB,
) extends ExpressionDetailsDao
    with StrictLogging {
  import scala.collection.JavaConverters._
  import ExpressionDetails._

  private val ageLimitMillis = config.getDuration("iep.lwc.fwding-admin.age-limit").toMillis

  private val table = new DynamoDB(dynamoDBClient).getTable(TableName)

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
        i.getMap[java.math.BigDecimal](Events).asScala.toMap.map {
          case (k, v) => (k, BigDecimal(v).toLong)
        },
        Option(i.getString(ScalingPolicy)).map(Json.decode[JsonNode](_))
      )
    }
  }

  override def scan(): List[ExpressionId] = {
    val spec = new ScanSpec()
      .withProjectionExpression(ExpressionIdAttr)

    val result = table.scan(spec)
    val idList = result
      .iterator()
      .asScala
      .map(item => Json.decode[ExpressionId](item.getString(ExpressionIdAttr)))
      .toList

    if (Option(
          result.getLastLowLevelResult.getScanResult.getLastEvaluatedKey
        ).isDefined) {
      logger.warn("Multiple pages found when querying for purge eligible expressions")
    }
    idList
  }

  override def queryPurgeEligible(now: Long, events: List[String]): List[ExpressionId] = {
    import scala.collection.JavaConverters._

    require(events.nonEmpty, s"Event markers required. Use $PurgeMarkerEvents")
    require(events.forall(PurgeMarkerEvents.contains), s"Invalid $events. Use: $PurgeMarkerEvents")

    val spec = new ScanSpec()
      .withProjectionExpression(ExpressionIdAttr)
      .withFilterExpression(events.map(e => s"$Events.$e < :ageThreshold").mkString(" OR "))
      .withValueMap(new ValueMap().withNumber(":ageThreshold", (now - ageLimitMillis)))

    val result = table.scan(spec)

    val idList = result
      .iterator()
      .asScala
      .map(item => Json.decode[ExpressionId](item.getString(ExpressionIdAttr)))
      .toList

    if (result.getLastLowLevelResult.getScanResult.getLastEvaluatedKey != null) {
      logger.warn("Multiple pages found when querying for purge eligible expressions")
    }

    idList
  }

  override def delete(id: ExpressionId): Unit = {
    table.deleteItem(new PrimaryKey(ExpressionIdAttr, Json.encode(id)))
  }

  override def isPurgeEligible(ed: ExpressionDetails, now: Long): Boolean = {
    ed.isPurgeEligible(now, ageLimitMillis)
  }

}

case class ExpressionDetails(
  expressionId: ExpressionId,
  timestamp: Long,
  fwdMetricInfo: Option[FwdMetricInfo],
  error: Option[Throwable],
  events: Map[String, Long],
  scalingPolicy: Option[JsonNode]
) {
  import ExpressionDetails._

  def isPurgeEligible(now: Long, ageLimitMillis: Long): Boolean = {
    val ageThreshold = now - ageLimitMillis
    events
      .filter { case (k, _) => PurgeMarkerEvents.contains(k) }
      .exists { case (_, v) => v < ageThreshold }
  }
}

object ExpressionDetails {
  val TableName = "iep.lwc.fwd.cw.ExpressionDetails"

  val ExpressionIdAttr = "ExpressionId"
  val Timestamp = "Timestamp"
  val FwdMetricInfoAttr = "FwdMetricInfo"
  val Error = "Error"
  val Events = "Events"
  val ScalingPolicy = "ScalingPolicy"

  val NoDataFoundEvent = "NoDataFoundEvent"
  val NoScalingPolicyFoundEvent = "NoScalingPolicyFoundEvent"
  val PurgeMarkerEvents = List(NoDataFoundEvent, NoScalingPolicyFoundEvent)
}

case class VersionMismatchException() extends Exception
