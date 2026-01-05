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

import java.util.regex.PatternSyntaxException
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.Dimension

class DefaultTaggerSuite extends FunSuite {

  private val dimensions = List(
    Dimension.builder().name("CloudWatch").value("abc").build(),
    Dimension.builder().name("ExtractDotDelimited").value("abc.def.captured-portion").build(),
    Dimension
      .builder()
      .name("ExtractSlashDelimited")
      .value("abc/captured-portion/42beef9876")
      .build(),
    Dimension.builder().name("NoMapping").value("def").build()
  )

  test("bad config") {
    val cfg = ConfigFactory.parseString("")
    intercept[ConfigException] {
      new DefaultTagger(cfg)
    }
  }

  test("add common tags") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = []
        |common-tags = [
        |  {
        |    key = "foo"
        |    value = "bar"
        |  }
        |]
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "foo"                   -> "bar",
      "CloudWatch"            -> "abc",
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("apply key mappings") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = [
        |  {
        |    name = "CloudWatch"
        |    alias = "InternalAlias"
        |  }
        |]
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "InternalAlias"         -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("extract value for configured keys") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+\\.[^.]+\\.(.*)"
        |      }
        |    ]
        |  },
        |  {
        |    name = "ExtractSlashDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+/([^.]+)/.*"
        |      }
        |    ]
        |  },
        |  {
        |    name = "MediaConnectARN"
        |    directives = [
        |      {
        |        pattern = "arn:aws:\\w+:([a-z\\-0-9]+):\\d+:source:\\d-[a-zA-Z0-9]+-[a-zA-Z0-9]+:(.*)"
        |      }
        |    ]
        |  }
        |]
        |mappings = []
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "captured-portion",
      "ExtractSlashDelimited" -> "captured-portion",
      "MediaConnectARN"       -> "us-west-1-test-Episode1-1-ABCDE",
      "CloudWatch"            -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    val withARN = dimensions ++ List(
      Dimension
        .builder()
        .name("MediaConnectARN")
        .value(
          "arn:aws:mediaconnect:us-west-1:123456789011:source:1-aAbBcCdDeEfFgGHh-aA913dabeef2:test-Episode1-1-ABCDE"
        )
        .build()
    )
    assertEquals(tagger(withARN), expected)
  }

  test("syntax error in extractor pattern throws") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+\\.[^.]+\\.(.*"
        |      }
        |    ]
        |  }
        |]
        |mappings = []
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    intercept[PatternSyntaxException] {
      new DefaultTagger(cfg)
    }
  }

  test("missing pattern in extractor throws") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    directives = [
        |      {
        |        alias = "new-name"
        |      }
        |    ]
        |  }
        |]
        |mappings = []
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    intercept[ConfigException.Missing] {
      new DefaultTagger(cfg)
    }
  }

  test("extractor without capture group returns raw value") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+\\.[^.]+\\..*"
        |      }
        |    ]
        |  }
        |]
        |mappings = []
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "CloudWatch"            -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("extract value and apply alias for configured keys") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractDotDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+\\.[^.]+\\.(.*)"
        |      }
        |    ]
        |  },
        |  {
        |    name = "ExtractSlashDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+/([^.]+)/.*"
        |        alias = "extracted"
        |      }
        |    ]
        |  }
        |]
        |mappings = []
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited" -> "captured-portion",
      "extracted"           -> "captured-portion",
      "CloudWatch"          -> "abc",
      "NoMapping"           -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("mapping is ignored if extractor of the same name exists") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractSlashDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+/([^.]+)/.*"
        |        alias = "extracted"
        |      }
        |    ]
        |  }
        |]
        |mappings = [
        |  {
        |    name = "ExtractSlashDelimited"
        |    alias = "ignored"
        |  }
        |]
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited" -> "abc.def.captured-portion",
      "extracted"           -> "captured-portion",
      "CloudWatch"          -> "abc",
      "NoMapping"           -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("first extractor pattern has precedence") {
    val cfg = ConfigFactory.parseString("""
        |extractors = [
        |  {
        |    name = "ExtractSlashDelimited"
        |    directives = [
        |      {
        |        pattern = "[^.]+/([^.]+)/.*"
        |        alias = "extracted"
        |      },
        |      {
        |        pattern = "[^.]+/[^.]+/(.*)"
        |        alias = "ignored"
        |      }
        |
        |    ]
        |  }
        |]
        |mappings = []
        |common-tags = []
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited" -> "abc.def.captured-portion",
      "extracted"           -> "captured-portion",
      "CloudWatch"          -> "abc",
      "NoMapping"           -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("dimensions override common tags") {
    val cfg = ConfigFactory.parseString("""
        |extractors = []
        |mappings = [
        |  {
        |    name = "CloudWatch"
        |    alias = "foo"
        |  }
        |]
        |common-tags = [
        |  {
        |    key = "foo"
        |    value = "bar"
        |  }
        |]
        |valid-tag-characters = "-._A-Za-z0-9"
        |valid-tag-value-characters = []
      """.stripMargin)

    val expected = Map(
      "ExtractDotDelimited"   -> "abc.def.captured-portion",
      "ExtractSlashDelimited" -> "abc/captured-portion/42beef9876",
      "foo"                   -> "abc",
      "NoMapping"             -> "def"
    )

    val tagger = new DefaultTagger(cfg)
    assertEquals(tagger(dimensions), expected)
  }

  test("aws.alb is assigned to LoadBalancer names starting with 'app' using production config") {
    val cfg = ConfigFactory.parseResources("reference.conf").resolve()
    val tagger = new DefaultTagger(cfg.getConfig("atlas.cloudwatch.tagger"))

    val expected = Map(
      "aws.alb" -> "captured-portion"
    )

    val actual = tagger(
      List(
        Dimension.builder().name("LoadBalancer").value("app/captured-portion/42beef9876").build()
      )
    )

    assertEquals(actual, expected)
  }

  test("aws.nlb is assigned to LoadBalancer names starting with 'net' using production config") {
    val cfg = ConfigFactory.parseResources("reference.conf").resolve()
    val tagger = new DefaultTagger(cfg.getConfig("atlas.cloudwatch.tagger"))

    val expected = Map(
      "aws.nlb" -> "captured-portion"
    )

    val actual = tagger(
      List(
        Dimension.builder().name("LoadBalancer").value("net/captured-portion/42beef9876").build()
      )
    )

    assertEquals(actual, expected)
  }
}
