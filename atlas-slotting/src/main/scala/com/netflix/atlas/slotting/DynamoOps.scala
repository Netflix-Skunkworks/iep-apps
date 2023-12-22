/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.slotting

import java.time.Duration
import com.netflix.iep.config.NetflixEnvironment
import com.netflix.spectator.api.Counter
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest

trait DynamoOps extends StrictLogging {

  val Name = "name"
  val Data = "data"
  val Active = "active"
  val Timestamp = "timestamp"

  private def javaMap[K, V](pairs: Seq[(K, V)]): java.util.Map[K, V] = {
    import scala.jdk.CollectionConverters.*
    new java.util.TreeMap[K, V](pairs.toMap.asJava)
  }

  private def createNameMap(pairs: (String, String)*): java.util.Map[String, String] = {
    javaMap[String, String](pairs)
  }

  private def createValueMap(
    pairs: (String, AttributeValue)*
  ): java.util.Map[String, AttributeValue] = {
    javaMap[String, AttributeValue](pairs)
  }

  private def binary(value: Array[Byte]): AttributeValue = {
    AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build()
  }

  private def bool(value: Boolean): AttributeValue = {
    AttributeValue.builder().bool(value).build()
  }

  private def number(value: Long): AttributeValue = {
    AttributeValue.builder().n(value.toString).build()
  }

  private def string(value: String): AttributeValue = {
    AttributeValue.builder().s(value).build()
  }

  def activeItemsScanRequest(table: String): ScanRequest = {
    val nameMap = createNameMap(
      "#a" -> Active
    )

    val valueMap = createValueMap(
      ":v1" -> bool(true)
    )

    ScanRequest
      .builder()
      .tableName(table)
      .filterExpression("#a = :v1")
      .expressionAttributeNames(nameMap)
      .expressionAttributeValues(valueMap)
      .build()
  }

  def oldItemsScanRequest(table: String, cutoffInterval: Duration): ScanRequest = {
    val cutoffMillis = System.currentTimeMillis() - cutoffInterval.toMillis

    val nameMap = createNameMap(
      "#n" -> Name,
      "#t" -> Timestamp
    )

    val valueMap = createValueMap(
      ":v1" -> number(cutoffMillis)
    )

    ScanRequest
      .builder()
      .tableName(table)
      .projectionExpression("#n")
      .filterExpression("#t < :v1")
      .expressionAttributeNames(nameMap)
      .expressionAttributeValues(valueMap)
      .build()
  }

  def deleteItemRequest(table: String, name: String): DeleteItemRequest = {
    DeleteItemRequest
      .builder()
      .tableName(table)
      .key(createValueMap(Name -> string(name)))
      .build()
  }

  def putAsgItemRequest(table: String, name: String, newData: Array[Byte]): PutItemRequest = {
    PutItemRequest
      .builder()
      .tableName(table)
      .item(
        createValueMap(
          Name      -> string(name),
          Data      -> binary(newData),
          Active    -> bool(true),
          Timestamp -> number(System.currentTimeMillis())
        )
      )
      .build()
  }

  def updateAsgItemRequest(
    table: String,
    name: String,
    oldData: Array[Byte],
    newData: Array[Byte]
  ): UpdateItemRequest = {
    val nameMap = createNameMap(
      "#d" -> Data,
      "#a" -> Active,
      "#t" -> Timestamp
    )

    val valueMap = createValueMap(
      ":v1" -> binary(oldData),
      ":v2" -> binary(newData),
      ":v3" -> bool(true),
      ":v4" -> number(System.currentTimeMillis())
    )

    UpdateItemRequest
      .builder()
      .tableName(table)
      .key(createValueMap(Name -> string(name)))
      .conditionExpression("#d = :v1")
      .updateExpression("set #d = :v2, #a = :v3, #t = :v4")
      .expressionAttributeNames(nameMap)
      .expressionAttributeValues(valueMap)
      .build()
  }

  def updateTimestampItemRequest(
    table: String,
    name: String,
    oldTimestamp: Long
  ): UpdateItemRequest = {
    val nameMap = createNameMap(
      "#a" -> Active,
      "#t" -> Timestamp
    )

    val valueMap = createValueMap(
      ":v1" -> number(oldTimestamp),
      ":v2" -> bool(true),
      ":v3" -> number(System.currentTimeMillis())
    )

    UpdateItemRequest
      .builder()
      .tableName(table)
      .key(createValueMap(Name -> string(name)))
      .conditionExpression("#t = :v1")
      .updateExpression("set #a = :v2, #t = :v3")
      .expressionAttributeNames(nameMap)
      .expressionAttributeValues(valueMap)
      .build()
  }

  def deactivateAsgItemRequest(table: String, name: String): UpdateItemRequest = {
    val nameMap = createNameMap(
      "#a" -> Active,
      "#t" -> Timestamp
    )

    val valueMap = createValueMap(
      ":v1" -> bool(true),
      ":v2" -> bool(false),
      ":v3" -> number(System.currentTimeMillis())
    )

    UpdateItemRequest
      .builder()
      .tableName(table)
      .key(createValueMap(Name -> string(name)))
      .conditionExpression("#a = :v1")
      .updateExpression("set #a = :v2, #t = :v3")
      .expressionAttributeNames(nameMap)
      .expressionAttributeValues(valueMap)
      .build()
  }

  def initTable(
    ddbClient: DynamoDbClient,
    tableName: String,
    desiredRead: Long,
    desiredWrite: Long,
    dynamodbErrors: Counter
  ): Unit = {

    logger.info(s"init dynamodb table $tableName in ${NetflixEnvironment.region()}")

    var continue = true

    while (continue) {
      try {
        val tableStatus = syncCapacity(ddbClient, tableName, desiredRead, desiredWrite)
        continue = tableStatus != "ACTIVE"
        Thread.sleep(1000)
      } catch {
        case _: ResourceNotFoundException =>
          createTable(ddbClient, tableName, desiredRead, desiredWrite)
          Thread.sleep(5000)
        case e: Exception =>
          logger.error(s"failed to update table $tableName: ${e.getMessage}", e)
          dynamodbErrors.increment()
          Thread.sleep(1000)
      }
    }
  }

  def syncCapacity(
    ddbClient: DynamoDbClient,
    tableName: String,
    desiredRead: Long,
    desiredWrite: Long
  ): String = {
    val request = DescribeTableRequest
      .builder()
      .tableName(tableName)
      .build()
    val table = ddbClient.describeTable(request).table()

    val currentRead = table.provisionedThroughput().readCapacityUnits()
    val currentWrite = table.provisionedThroughput().writeCapacityUnits()

    if (currentWrite != desiredWrite || currentRead != desiredRead) {
      val throughput = ProvisionedThroughput
        .builder()
        .readCapacityUnits(desiredRead)
        .writeCapacityUnits(desiredWrite)
        .build()

      val request = UpdateTableRequest
        .builder()
        .tableName(tableName)
        .provisionedThroughput(throughput)
        .build()

      logger.info(
        s"update capacity region=${NetflixEnvironment.region()} " +
          s"tableName=$tableName read=$desiredRead write=$desiredWrite"
      )

      ddbClient.updateTable(request)
    }

    table.tableStatusAsString()
  }

  def createTable(
    ddbClient: DynamoDbClient,
    tableName: String,
    desiredRead: Long,
    desiredWrite: Long
  ): Unit = {
    val keySchema = KeySchemaElement
      .builder()
      .attributeName("name")
      .keyType(KeyType.HASH)
      .build()

    val attrDef = AttributeDefinition
      .builder()
      .attributeName("name")
      .attributeType("S")
      .build()

    val throughput = ProvisionedThroughput
      .builder()
      .readCapacityUnits(desiredRead)
      .writeCapacityUnits(desiredWrite)
      .build()

    val request = CreateTableRequest
      .builder()
      .tableName(tableName)
      .keySchema(keySchema)
      .attributeDefinitions(attrDef)
      .provisionedThroughput(throughput)
      .build()

    logger.info(
      s"create table region=${NetflixEnvironment.region()} " +
        s"tableName=$tableName read=$desiredRead write=$desiredWrite"
    )

    ddbClient.createTable(request)
  }
}
