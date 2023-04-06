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
package com.netflix.atlas.cloudwatch

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.config.ConfigClient
import software.amazon.awssdk.services.config.model.SelectAggregateResourceConfigRequest
import software.amazon.awssdk.services.config.model.SelectAggregateResourceConfigResponse
import software.amazon.awssdk.services.config.paginators.SelectAggregateResourceConfigIterable

import java.util.Optional

class AwsConfigAccountSupplierSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem("Test")

  var registry: Registry = null
  var clientFactory: AwsClientFactory = null
  var client: ConfigClient = null
  var config: Config = null

  override def beforeEach(context: BeforeEach): Unit = {
    registry = new DefaultRegistry()
    clientFactory = mock[AwsClientFactory]
    client = mock[ConfigClient]
    when(
      clientFactory.getInstance(
        anyString,
        any[Class[ConfigClient]],
        anyString,
        any[Optional[Region]]
      )
    ).thenReturn(client)
    config = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.supplier.aws {
        |  aggregator = "agg1"
        |  aggregator-region = "us-west-2"
        |  regions = ["us-west-1", "us-west-2"]
        |  config-account = "042"
        |  is-allow = false
        |  accounts = []
        |}
      """.stripMargin)
  }

  test("success") {
    mockResp()
    val accts = new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    assertEquals(accts.filtered.size, 3)
    var regions = accts.filtered("123")
    assertEquals(regions.size, 2)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/EC2", "AWS/S3"))
    assertEquals(regions(Region.US_WEST_2), Set("AWS/EC2"))
    regions = accts.filtered("456")
    assertEquals(regions.size, 2)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/RDS", "AWS/S3"))
    assertEquals(regions(Region.US_WEST_2), Set("AWS/S3"))
    regions = accts.filtered("789")
    assertEquals(regions.size, 1)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/EC2"))
    assertCounters()
  }

  test("empty response") {
    mockResp(empty = true)
    val accts = new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    assertEquals(accts.filtered.size, 0)
    assertCounters()
  }

  test("exception on startup") {
    mockResp(exception = true)
    intercept[RuntimeException] {
      new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    }
    assertCounters(ex = 1)
  }

  test("malformed") {
    mockResp(malformed = true)
    val accts = new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    assertEquals(accts.filtered.size, 3)
    var regions = accts.filtered("123")
    assertEquals(regions.size, 2)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/EC2", "AWS/S3"))
    assertEquals(regions(Region.US_WEST_2), Set("AWS/EC2"))
    regions = accts.filtered("456")
    assertEquals(regions.size, 2)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/RDS"))
    assertEquals(regions(Region.US_WEST_2), Set("AWS/S3"))
    regions = accts.filtered("789")
    assertEquals(regions.size, 1)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/EC2"))
    assertCounters(parse = 1)
  }

  test("allow list") {
    config = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.supplier.aws {
        |  aggregator = "agg1"
        |  aggregator-region = "us-west-2"
        |  regions = ["us-west-1", "us-west-2"]
        |  config-account = "042"
        |  is-allow = true
        |  accounts = ["456"]
        |}
      """.stripMargin)
    mockResp()
    val accts = new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    assertEquals(accts.filtered.size, 1)
    val regions = accts.filtered("456")
    assertEquals(regions.size, 2)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/RDS", "AWS/S3"))
    assertEquals(regions(Region.US_WEST_2), Set("AWS/S3"))
  }

  test("deny list") {
    config = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.supplier.aws {
        |  aggregator = "agg1"
        |  aggregator-region = "us-west-2"
        |  regions = ["us-west-1", "us-west-2"]
        |  config-account = "042"
        |  is-allow = false
        |  accounts = ["456"]
        |}
      """.stripMargin)
    mockResp()
    val accts = new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    assertEquals(accts.filtered.size, 2)
    var regions = accts.filtered("123")
    assertEquals(regions.size, 2)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/EC2", "AWS/S3"))
    assertEquals(regions(Region.US_WEST_2), Set("AWS/EC2"))
    regions = accts.filtered("789")
    assertEquals(regions.size, 1)
    assertEquals(regions(Region.US_WEST_1), Set("AWS/EC2"))
    assertCounters()
  }

  test("filter global") {
    config = ConfigFactory.parseString("""
        |atlas.cloudwatch.account.supplier.aws {
        |  aggregator = "agg1"
        |  aggregator-region = "us-west-2"
        |  regions = ["global"]
        |  config-account = "042"
        |  is-allow = false
        |  accounts = []
        |}
      """.stripMargin)
    mockResp()
    val accts = new AwsConfigAccountSupplier(config, registry, clientFactory)(system)
    assertEquals(accts.filtered.size, 1)
    val regions = accts.filtered("456")
    assertEquals(regions.size, 1)
    assertEquals(regions(Region.AWS_GLOBAL), Set("AWS/IAM"))
  }

  def mockResp(
    empty: Boolean = false,
    exception: Boolean = false,
    malformed: Boolean = false
  ): Unit = {
    val resp = SelectAggregateResourceConfigResponse
      .builder()
      .results(
        "{\"COUNT(*)\":16,\"accountId\":\"123\",\"resourceType\":\"AWS::EC2::InternetGateway\",\"awsRegion\":\"us-west-1\"}",
        "{\"COUNT(*)\":16,\"accountId\":\"123\",\"resourceType\":\"AWS::S3::Bucket\",\"awsRegion\":\"us-west-1\"}",
        "{\"COUNT(*)\":2,\"accountId\":\"123\",\"resourceType\":\"AWS::EC2::InternetGateway\",\"awsRegion\":\"us-west-2\"}",
        "{\"COUNT(*)\":8,\"accountId\":\"456\",\"resourceType\":\"AWS::RDS::DBSecurityGroup\",\"awsRegion\":\"us-west-1\"}",
        "{\"COUNT(*)\":8,\"accountId\":\"456\",\"resourceType\":\"AWS::IAM::Policy\",\"awsRegion\":\"global\"}",
        "{\"COUNT(*)\":3,\"accountId\":\"456\",\"resourceType\":\"AWS::S3::Bucket\",\"awsRegion\":\"us-west-2\"}",
        if (malformed) "{\"COUNT(*)\":2,\"accountId\":\"456\",\"reso"
        else
          "{\"COUNT(*)\":2,\"accountId\":\"456\",\"resourceType\":\"AWS::S3::Bucket\",\"awsRegion\":\"us-west-1\"}",
        "{\"COUNT(*)\":20,\"accountId\":\"789\",\"resourceType\":\"AWS::EC2::InternetGateway\",\"awsRegion\":\"us-west-1\"}"
      )
      .build
    val emptyResp = SelectAggregateResourceConfigResponse.builder().build
    if (exception) {
      when(client.selectAggregateResourceConfig(any[SelectAggregateResourceConfigRequest]))
        .thenThrow(new RuntimeException("test"))
    } else if (empty) {
      when(client.selectAggregateResourceConfig(any[SelectAggregateResourceConfigRequest]))
        .thenReturn(emptyResp)
    } else {
      when(client.selectAggregateResourceConfig(any[SelectAggregateResourceConfigRequest]))
        .thenReturn(resp)
    }
    when(client.selectAggregateResourceConfigPaginator(any[SelectAggregateResourceConfigRequest]))
      .thenAnswer((req: SelectAggregateResourceConfigRequest) =>
        new SelectAggregateResourceConfigIterable(client, req)
      )
  }

  def assertCounters(
    parse: Long = 0,
    ex: Long = 0
  ): Unit = {
    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.account.supplier.aws.parseException",
          "exception",
          "JsonEOFException"
        )
        .count(),
      parse
    )
    assertEquals(
      registry
        .counter(
          "atlas.cloudwatch.account.supplier.aws.loadException",
          "exception",
          "RuntimeException"
        )
        .count(),
      ex
    )
  }
}
