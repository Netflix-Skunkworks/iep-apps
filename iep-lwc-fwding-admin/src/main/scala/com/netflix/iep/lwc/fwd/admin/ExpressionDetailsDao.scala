/*
 * Copyright 2014-2025 Netflix, Inc.
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

import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.admin.Timer.*
import com.netflix.iep.lwc.fwd.cw.*
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

trait ExpressionDetailsDao {

  def save(exprDetails: ExpressionDetails): Unit
  def read(id: ExpressionId): Option[ExpressionDetails]
  def scan(): List[ExpressionId]
  def queryPurgeEligible(now: Long, events: List[String]): List[ExpressionId]
  def delete(id: ExpressionId): Unit
  def isPurgeEligible(ed: ExpressionDetails, now: Long): Boolean
}

class ExpressionDetailsDaoImpl(
  config: Config,
  dynamoDBClient: DynamoDbClient,
  registry: Registry
) extends ExpressionDetailsDao
    with StrictLogging {

  import ExpressionDetails.*

  import scala.jdk.CollectionConverters.*
  import scala.jdk.StreamConverters.*

  private val ageLimitMillis = config.getDuration("iep.lwc.fwding-admin.age-limit").toMillis

  private def javaMap[K, V](pairs: Seq[(K, V)]): java.util.Map[K, V] = {
    pairs.toMap.asJava
  }

  private def createValueMap(
    pairs: (String, AttributeValue)*
  ): java.util.Map[String, AttributeValue] = {
    javaMap[String, AttributeValue](pairs)
  }

  private def number(value: Long): AttributeValue = {
    AttributeValue.builder().n(value.toString).build()
  }

  private def string(value: String): AttributeValue = {
    AttributeValue.builder().s(value).build()
  }

  private def map(value: Map[String, Long]): AttributeValue = {
    val m = new java.util.HashMap[String, AttributeValue]()
    value.foreachEntry { (k, v) =>
      m.put(k, number(v))
    }
    AttributeValue.builder().m(m).build()
  }

  override def save(exprDetails: ExpressionDetails): Unit = {
    val item = createValueMap(
      ExpressionIdAttr    -> string(Json.encode(exprDetails.expressionId)),
      Timestamp           -> number(exprDetails.timestamp),
      Events              -> map(exprDetails.events),
      ForwardedMetrics    -> string(Json.encode(exprDetails.forwardedMetrics)),
      ScalingPoliciesAttr -> string(Json.encode(exprDetails.scalingPolicies))
    )

    exprDetails.error.foreach { e =>
      item.put(Error, string(Json.encode(e)))
    }

    val request = PutItemRequest
      .builder()
      .tableName(TableName)
      .item(item)
      .build()
    record(dynamoDBClient.putItem(request), "fwdingAdminSaveTimer", registry)
  }

  override def read(id: ExpressionId): Option[ExpressionDetails] = {
    val request = GetItemRequest
      .builder()
      .tableName(TableName)
      .key(createValueMap(ExpressionIdAttr -> string(Json.encode(id))))
      .build()
    val itemResponse = record(
      dynamoDBClient.getItem(request),
      "fwdingAdminReadTimer",
      registry
    )

    if (itemResponse.hasItem) {
      val item = itemResponse.item()
      val details = new ExpressionDetails(
        id,
        item.get(Timestamp).n().toLong,
        Json.decode[List[FwdMetricInfo]](item.get(ForwardedMetrics).s()),
        Option(item.get(Error)).map(v => Json.decode[Throwable](v.s())),
        item.get(Events).m().asScala.toMap.map {
          case (k, v) => k -> v.n().toLong
        },
        Json.decode[List[ScalingPolicy]](item.get(ScalingPoliciesAttr).s())
      )
      Some(details)
    } else {
      None
    }
  }

  override def scan(): List[ExpressionId] = {
    val request = ScanRequest
      .builder()
      .tableName(TableName)
      .projectionExpression(ExpressionIdAttr)
      .build()

    dynamoDBClient
      .scanPaginator(request)
      .items()
      .stream()
      .map { item =>
        Json.decode[ExpressionId](item.get(ExpressionIdAttr).s())
      }
      .toScala(List)
  }

  override def queryPurgeEligible(now: Long, events: List[String]): List[ExpressionId] = {
    require(events.nonEmpty, s"Event markers required. Use $PurgeMarkerEvents")
    require(events.forall(PurgeMarkerEvents.contains), s"Invalid $events. Use: $PurgeMarkerEvents")

    val request = ScanRequest
      .builder()
      .tableName(TableName)
      .projectionExpression(ExpressionIdAttr)
      .filterExpression(events.map(e => s"$Events.$e < :ageThreshold").mkString(" OR "))
      .expressionAttributeValues(
        createValueMap(
          ":ageThreshold" -> number(now - ageLimitMillis)
        )
      )
      .build()

    dynamoDBClient
      .scanPaginator(request)
      .items()
      .stream()
      .map { item =>
        Json.decode[ExpressionId](item.get(ExpressionIdAttr).s())
      }
      .toScala(List)
  }

  override def delete(id: ExpressionId): Unit = {
    val request = DeleteItemRequest
      .builder()
      .tableName(TableName)
      .key(createValueMap(ExpressionIdAttr -> string(Json.encode(id))))
      .build()
    dynamoDBClient.deleteItem(request)
  }

  override def isPurgeEligible(ed: ExpressionDetails, now: Long): Boolean = {
    ed.isPurgeEligible(now, ageLimitMillis)
  }

}

case class ExpressionDetails(
  expressionId: ExpressionId,
  timestamp: Long,
  forwardedMetrics: List[FwdMetricInfo],
  error: Option[Throwable],
  events: Map[String, Long],
  scalingPolicies: List[ScalingPolicy]
) {

  import ExpressionDetails.*

  def isPurgeEligible(now: Long, ageLimitMillis: Long): Boolean = {
    val ageThreshold = now - ageLimitMillis
    events
      .filter { case (k, _) => PurgeMarkerEvents.contains(k) }
      .exists { case (_, v) => v < ageThreshold }
  }
}

case class ScalingPolicyStatus(
  unknown: Boolean,
  scalingPolicy: Option[ScalingPolicy]
)

object ExpressionDetails {

  val TableName = "iep.lwc.fwd.cw.ExpressionDetails"

  val ExpressionIdAttr = "ExpressionId"
  val Timestamp = "Timestamp"
  val ForwardedMetrics = "ForwardedMetrics"
  val Error = "Error"
  val Events = "Events"
  val ScalingPoliciesAttr = "ScalingPolicies"

  val NoDataFoundEvent = "NoDataFoundEvent"
  val NoScalingPolicyFoundEvent = "NoScalingPolicyFoundEvent"
  val PurgeMarkerEvents = List(NoDataFoundEvent, NoScalingPolicyFoundEvent)
}
