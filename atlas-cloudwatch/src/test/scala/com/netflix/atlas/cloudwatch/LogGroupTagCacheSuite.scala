/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.iep.aws2.AwsClientFactory
import munit.FunSuite
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model.ListTagsForResourceRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.ListTagsForResourceResponse

import java.util.Optional
import scala.jdk.CollectionConverters.*

class LogGroupTagCacheSuite extends FunSuite {

  private val account = "123456789012"
  private val region = "us-east-1"
  private val logGroup = "/aws/lambda/my-function"
  private val expectedArn = s"arn:aws:logs:$region:$account:log-group:$logGroup"

  private def makeCache(client: CloudWatchLogsClient): LogGroupTagCache = {
    val factory = mock(classOf[AwsClientFactory])
    when(
      factory.getInstance(
        anyString,
        any[Class[CloudWatchLogsClient]],
        anyString,
        any[Optional[Region]]
      )
    ).thenReturn(client)
    new LogGroupTagCache(factory)
  }

  private def mockClientWithTags(tags: Map[String, String]): CloudWatchLogsClient = {
    val client = mock(classOf[CloudWatchLogsClient])
    val response = ListTagsForResourceResponse
      .builder()
      .tags(tags.asJava)
      .build()
    when(client.listTagsForResource(any[ListTagsForResourceRequest])).thenReturn(response)
    client
  }

  test("returns tags from AWS on first call") {
    val client = mockClientWithTags(Map("env" -> "prod", "team" -> "sre"))
    val cache = makeCache(client)

    val tags = cache.getTags(logGroup, account, region)
    assertEquals(tags, Map("env" -> "prod", "team" -> "sre"))
  }

  test("returns empty map when log group has no tags") {
    val client = mockClientWithTags(Map.empty)
    val cache = makeCache(client)

    val tags = cache.getTags(logGroup, account, region)
    assertEquals(tags, Map.empty[String, String])
  }

  test("caches result and does not call AWS on second call for same log group") {
    val client = mockClientWithTags(Map("env" -> "prod"))
    val cache = makeCache(client)

    cache.getTags(logGroup, account, region)
    cache.getTags(logGroup, account, region)

    verify(client, times(1)).listTagsForResource(any[ListTagsForResourceRequest])
  }

  test("calls AWS separately for different log groups") {
    val client = mockClientWithTags(Map("env" -> "prod"))
    val cache = makeCache(client)

    cache.getTags("/aws/lambda/fn-a", account, region)
    cache.getTags("/aws/lambda/fn-b", account, region)

    verify(client, times(2)).listTagsForResource(any[ListTagsForResourceRequest])
  }

  test("returns empty map and caches it on AWS exception") {
    val client = mock(classOf[CloudWatchLogsClient])
    when(client.listTagsForResource(any[ListTagsForResourceRequest]))
      .thenThrow(new RuntimeException("Access denied"))
    val cache = makeCache(client)

    val tags = cache.getTags(logGroup, account, region)
    assertEquals(tags, Map.empty[String, String])

    // Second call should not hit AWS again
    cache.getTags(logGroup, account, region)
    verify(client, times(1)).listTagsForResource(any[ListTagsForResourceRequest])
  }

  test("uses correct ARN format in the request") {
    val client = mockClientWithTags(Map.empty)
    val cache = makeCache(client)
    cache.getTags(logGroup, account, region)

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[ListTagsForResourceRequest])
    verify(client).listTagsForResource(captor.capture())
    assertEquals(captor.getValue.resourceArn(), expectedArn)
  }
}
