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
package com.netflix.atlas.slotting

import java.nio.ByteBuffer
import java.time.Duration

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.NameMap
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Counter
import com.typesafe.scalalogging.StrictLogging

trait DynamoOps extends StrictLogging {

  val Name = "name"
  val Data = "data"
  val Active = "active"
  val Timestamp = "timestamp"

  def activeItemsScanSpec(): ScanSpec = {
    val nameMap = new NameMap()
      .`with`("#a", Active)

    val valueMap = new ValueMap()
      .withBoolean(":v1", true)

    new ScanSpec()
      .withFilterExpression("#a = :v1")
      .withNameMap(nameMap)
      .withValueMap(valueMap)
  }

  def oldItemsScanSpec(cutoffInterval: Duration): ScanSpec = {
    val cutoffMillis = System.currentTimeMillis() - cutoffInterval.toMillis

    val nameMap = new NameMap()
      .`with`("#n", Name)
      .`with`("#t", Timestamp)

    val valueMap = new ValueMap()
      .withLong(":v1", cutoffMillis)

    new ScanSpec()
      .withProjectionExpression("#n")
      .withFilterExpression("#t < :v1")
      .withNameMap(nameMap)
      .withValueMap(valueMap)
  }

  def newAsgItem(name: String, newData: ByteBuffer): Item = {
    new Item()
      .withPrimaryKey(Name, name)
      .withBinary(Data, newData)
      .withBoolean(Active, true)
      .withLong(Timestamp, System.currentTimeMillis)
  }

  def updateAsgItemSpec(name: String, oldData: ByteBuffer, newData: ByteBuffer): UpdateItemSpec = {
    val nameMap = new NameMap()
      .`with`("#d", Data)
      .`with`("#a", Active)
      .`with`("#t", Timestamp)

    val valueMap = new ValueMap()
      .withBinary(":v1", Util.toByteArray(oldData))
      .withBinary(":v2", Util.toByteArray(newData))
      .withBoolean(":v3", true)
      .withLong(":v4", System.currentTimeMillis)

    new UpdateItemSpec()
      .withPrimaryKey(Name, name)
      .withConditionExpression("#d = :v1")
      .withUpdateExpression("set #d = :v2, #a = :v3, #t = :v4")
      .withNameMap(nameMap)
      .withValueMap(valueMap)
  }

  def updateTimestampItemSpec(name: String, oldTimestamp: Long): UpdateItemSpec = {
    val nameMap = new NameMap()
      .`with`("#a", Active)
      .`with`("#t", Timestamp)

    val valueMap = new ValueMap()
      .withLong(":v1", oldTimestamp)
      .withBoolean(":v2", true)
      .withLong(":v3", System.currentTimeMillis)

    new UpdateItemSpec()
      .withPrimaryKey(Name, name)
      .withConditionExpression("#t = :v1")
      .withUpdateExpression("set #a = :v2, #t = :v3")
      .withNameMap(nameMap)
      .withValueMap(valueMap)
  }

  def deactivateAsgItemSpec(name: String): UpdateItemSpec = {
    val nameMap = new NameMap()
      .`with`("#a", Active)
      .`with`("#t", Timestamp)

    val valueMap = new ValueMap()
      .withBoolean(":v1", true)
      .withBoolean(":v2", false)
      .withLong(":v3", System.currentTimeMillis)

    new UpdateItemSpec()
      .withPrimaryKey(Name, name)
      .withConditionExpression("#a = :v1")
      .withUpdateExpression("set #a = :v2, #t = :v3")
      .withNameMap(nameMap)
      .withValueMap(valueMap)
  }

  def initTable(
    ddbClient: AmazonDynamoDB,
    tableName: String,
    desiredRead: Long,
    desiredWrite: Long,
    dynamodbErrors: Counter
  ): Unit = {
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
    ddbClient: AmazonDynamoDB,
    tableName: String,
    desiredRead: Long,
    desiredWrite: Long
  ): String = {
    val request = new DescribeTableRequest().withTableName(tableName)
    val table = ddbClient.describeTable(request).getTable

    val currentRead = table.getProvisionedThroughput.getReadCapacityUnits
    val currentWrite = table.getProvisionedThroughput.getWriteCapacityUnits

    if (currentWrite != desiredWrite || currentRead != desiredRead) {
      val throughput = new ProvisionedThroughput()
        .withReadCapacityUnits(desiredRead)
        .withWriteCapacityUnits(desiredWrite)

      val request = new UpdateTableRequest()
        .withTableName(tableName)
        .withProvisionedThroughput(throughput)

      logger.info(
        s"update capacity region=${NetflixEnvironment.region()} " +
        s"tableName=$tableName read=$desiredRead write=$desiredWrite"
      )

      ddbClient.updateTable(request)
    }

    table.getTableStatus
  }

  def createTable(
    ddbClient: AmazonDynamoDB,
    tableName: String,
    desiredRead: Long,
    desiredWrite: Long
  ): Unit = {
    val keySchema = new KeySchemaElement()
      .withAttributeName("name")
      .withKeyType(KeyType.HASH)

    val attrDef = new AttributeDefinition()
      .withAttributeName("name")
      .withAttributeType("S")

    val throughput = new ProvisionedThroughput()
      .withReadCapacityUnits(desiredRead)
      .withWriteCapacityUnits(desiredWrite)

    val request = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(keySchema)
      .withAttributeDefinitions(attrDef)
      .withProvisionedThroughput(throughput)

    logger.info(
      s"create table region=${NetflixEnvironment.region()} " +
      s"tableName=$tableName read=$desiredRead write=$desiredWrite"
    )

    ddbClient.createTable(request)
  }
}
