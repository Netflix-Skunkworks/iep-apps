/*
 * Copyright 2014-2024 Netflix, Inc.
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

import java.time.Duration
import com.netflix.atlas.core.model.Query
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import junit.framework.TestCase.assertFalse
import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.Dimension

class MetricCategorySuite extends FunSuite {

  test("bad config") {
    val cfg = ConfigFactory.empty()
    intercept[ConfigException] {
      MetricCategory.fromConfig(cfg)
    }
  }

  test("load category with empty dimensions succeeds") {
    val cfg = ConfigFactory.parseString("""
        |      namespace = "AWS/Lambda"
        |      period = 1m
        |
        |      dimensions = []
        |
        |      metrics = [
        |        {
        |          name = "UnreservedConcurrentExecutions"
        |          alias = "aws.lambda.concurrentExecutions"
        |          conversion = "max"
        |          tags = [
        |            {
        |              key = "concurrencyLimit"
        |              value = "unreserved"
        |            }
        |          ]
        |        },
        |      ]
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertEquals(category.namespace, "AWS/Lambda")
    assert(category.dimensions.isEmpty)
  }

  test("load category with no dimensions throws") {
    val cfg = ConfigFactory.parseString("""
        |      namespace = "AWS/Lambda"
        |      period = 1m
        |
        |      metrics = [
        |        {
        |          name = "UnreservedConcurrentExecutions"
        |          alias = "aws.lambda.concurrentExecutions"
        |          conversion = "max"
        |          tags = [
        |            {
        |              key = "concurrencyLimit"
        |              value = "unreserved"
        |            }
        |          ]
        |        },
        |      ]
      """.stripMargin)

    intercept[ConfigException.Missing] {
      MetricCategory.fromConfig(cfg)
    }
  }

  test("load from config") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate"
        |  },
        |  {
        |    name = "HTTPCode_ELB_4XX"
        |    alias = "aws.elb.errors"
        |    conversion = "sum,rate"
        |    tags = [
        |      {
        |        key = "status"
        |        value = "4xx"
        |      }
        |    ]
        |  }
        |]
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertEquals(category.namespace, "AWS/ELB")
    assertEquals(category.period, 60)
    assertEquals(category.toListRequests.size, 2)
    assertEquals(category.filter, None)
  }

  test("category without grace override") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = []
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertEquals(category.graceOverride, -1)
  }

  test("category with grace override") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |grace-override = 4
        |dimensions = ["LoadBalancerName"]
        |metrics = []
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertEquals(category.graceOverride, 4)
  }

  test("config with filter") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate"
        |  }
        |]
        |filter = "name,RequestCount,:eq"
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertEquals(category.filter, Some(Query.Equal("name", "RequestCount")))
  }

  test("config with invalid filter") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate"
        |  }
        |]
        |filter = "name,:invalid-command"
      """.stripMargin)

    intercept[IllegalStateException] {
      MetricCategory.fromConfig(cfg)
    }
  }

  test("category with monotonic") {
    val cfg = ConfigFactory.parseString("""
        namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate",
        |    monotonic = true
        |  }
        |]
        |filter = "name,RequestCount,:eq"
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assert(category.hasMonotonic)
  }

  test("category with poll offset") {
    val cfg = ConfigFactory.parseString("""
        namespace = "AWS/ELB"
        |period = 1m
        |poll-offset = 7h
        |dimensions = ["LoadBalancerName"]
        |metrics = [
        |  {
        |    name = "RequestCount"
        |    alias = "aws.elb.requests"
        |    conversion = "sum,rate",
        |    monotonic = true
        |  }
        |]
        |filter = "name,RequestCount,:eq"
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertEquals(category.pollOffset.get, Duration.ofHours(7))
  }

  test("dimensionsMatch true") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = []
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assert(
      category.dimensionsMatch(
        List(
          Dimension.builder().name("LoadBalancerName").value("UT").build()
        )
      )
    )
  }

  test("dimensionsMatch true ignore nf.*") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = []
        """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assert(
      category.dimensionsMatch(
        List(
          Dimension.builder().name("LoadBalancerName").value("UT").build(),
          Dimension.builder().name("nf.region").value("us-west-2").build(),
          Dimension.builder().name("nf.account").value("1234").build()
        )
      )
    )
  }

  test("dimensionsMatch too few") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = []
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertFalse(category.dimensionsMatch(List.empty))
  }

  test("dimensionsMatch too many") {
    val cfg = ConfigFactory.parseString("""
        |namespace = "AWS/ELB"
        |period = 1 m
        |dimensions = ["LoadBalancerName"]
        |metrics = []
      """.stripMargin)

    val category = MetricCategory.fromConfig(cfg)
    assertFalse(
      category.dimensionsMatch(
        List(
          Dimension.builder().name("LoadBalancerName").value("UT").build(),
          Dimension.builder().name("ExtraDimension").value("UT").build()
        )
      )
    )
  }
}
