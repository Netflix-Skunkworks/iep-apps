/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.aggregator

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.IdMap
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.validation.CompositeTagRule
import com.netflix.atlas.core.validation.Rule
import com.netflix.atlas.core.validation.TagRule
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.iep.NetflixEnvironment
import com.netflix.iep.config.ConfigListener
import com.netflix.iep.config.ConfigManager
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Tag
import com.netflix.spectator.atlas.impl.Parser
import com.netflix.spectator.atlas.impl.Query
import com.netflix.spectator.atlas.impl.QueryIndex
import com.netflix.spectator.impl.AsciiSet
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

/**
  * Helper for encoding and decoding the aggregator payloads. Format of the payload is a simple
  * array containing:
  *
  * - String array
  *     - Int for the number of string values
  *     - String values used in subsequent ids
  * - Meter updates, repeated until end of array
  *     - Int for the number of tags
  *     - For each tag, two ints representing the index in the string array for the key
  *       and value
  *     - Int indicating the operation, 0 means add to counter, 10 means max applied to
  *       max gauge
  *     - Double indicating the value to use when updating the meter
  *
  * @param replaceInvalidCharactersFunction
  *     Map invalid characters in the input string to a replacement character. The spectator
  *     registry will do this internally when publishing, but it is done while parsing to ensure
  *     the expected aggregate is received and there is no deduping.
  * @param validator
  *     Rules used to validate the key/value pairs for the tag maps.
  * @param rollupFunction
  *     Performs rollups if necessary to remove problematic dimensions on the input ids.
  */
class PayloadDecoder(
  replaceInvalidCharactersFunction: String => String,
  validator: TagRule,
  rollupFunction: Id => Id
) extends StrictLogging {

  import PayloadDecoder._
  import com.netflix.atlas.json.JsonParserHelper._

  /**
    * Decode payload and send the updates to the aggregator implementation.
    *
    * @param parser
    *     Parser to use for reading the payload.
    * @param aggregator
    *     Handler that will receive the updates.
    * @return
    *     Indicates whether or not all of the datapoints in the payload were processed
    *     successfully.
    */
  def decode(parser: JsonParser, aggregator: Aggregator): Result = {
    val validationResults = List.newBuilder[ValidationResult]
    var numDatapoints = 0

    requireNextToken(parser, JsonToken.START_ARRAY)
    val numStrings = nextInt(parser)
    val strings = loadStringTable(numStrings, parser)
    val nameIdx = findName(strings)

    var t = parser.nextToken()
    while (t != null && t != JsonToken.END_ARRAY) {
      numDatapoints += 1
      val numTags = parser.getIntValue
      val result = loadTags(numTags, strings, nameIdx, parser)
      val op = nextInt(parser)
      val value = nextDouble(parser)
      result match {
        case Left(vr) =>
          validationResults += vr
        case Right(id) =>
          op match {
            case ADD =>
              // Add the aggr tag to avoid values getting deduped on the backend
              logger.debug(s"received updated, ADD $id $value")
              aggregator.add(id.withTag(aggrTag), value)
            case MAX =>
              logger.debug(s"received updated, MAX $id $value")
              aggregator.max(id, value)
            case unk =>
              throw new IllegalArgumentException(
                s"unknown operation $unk, expected add ($ADD) or max ($MAX)"
              )
          }
      }
      t = parser.nextToken()
    }
    Result(numDatapoints, validationResults.result())
  }

  private def loadStringTable(n: Int, parser: JsonParser): Array[String] = {
    val strings = new Array[String](n)
    var i = 0
    while (i < n) {
      strings(i) = replaceInvalidCharactersFunction(nextString(parser))
      i += 1
    }
    strings
  }

  private def findName(strings: Array[String]): Int = {
    var i = 0
    while (i < strings.length) {
      if (strings(i) == "name") return i
      i += 1
    }
    -1
  }

  private def loadTags(
    n: Int,
    strings: Array[String],
    nameIdx: Int,
    parser: JsonParser
  ): Either[ValidationResult, Id] = {
    var result: String = TagRule.Pass
    var numUserTags = 0
    var name: String = null
    val tags = new Array[String](2 * n)
    var pos = 0
    var i = 0
    while (i < n) {
      val ki = nextInt(parser)
      val k = strings(ki)
      val v = strings(nextInt(parser))
      if (ki == nameIdx) {
        // Needs to be done before pass check so that it will not get
        // incorrectly flagged as missing name if another check fails
        name = v
        numUserTags += 1
      } else if (result == TagRule.Pass) {
        // Avoid doing unnecessary work if it has already failed validation. We still need
        // to process the entry as subsequent entries may be fine.
        tags(pos) = k
        tags(pos + 1) = v
        pos += 2
        if (!TagKey.isRestricted(k)) {
          numUserTags += 1
        }
        result = validator.validate(k, v)
      }
      i += 1
    }

    if (name == null) {
      // This should never happen if clients are working properly
      val builder = new SmallHashMap.Builder[String, String](n)
      var i = 0
      while (i < pos) {
        builder.add(tags(i), tags(i + 1))
        i += 2
      }
      Left(ValidationResult.Fail("HasKeyRule", s"missing key 'name'", builder.result))
    } else {
      val id = Id.unsafeCreate(name, tags, pos)
      if (result == TagRule.Pass) {
        // Validation of individual key/value pairs passed, now check rules for the
        // overall tag set
        if (numUserTags > maxUserTags) {
          Left(
            ValidationResult.Fail(
              "MaxUserTagsRule",
              s"too many user tags: $numUserTags > $maxUserTags",
              IdMap(id)
            )
          )
        } else {
          Right(rollupFunction(id))
        }
      } else {
        // Tag rule check failed
        Left(ValidationResult.Fail("KeyValueRule", result, IdMap(id)))
      }
    }
  }
}

object PayloadDecoder {

  case object NoValidation extends TagRule {
    override def validate(k: String, v: String): String = TagRule.Pass
  }

  case class Result(numDatapoints: Int, failures: List[ValidationResult])

  case class RollupPolicy(query: Query, rollup: java.util.List[String])

  private val ADD = 0
  private val MAX = 10

  private val config = ConfigManager.get().getConfig("atlas.aggregator")

  @volatile private var rollupPolicies = QueryIndex.newInstance[RollupPolicy](new NoopRegistry)

  ConfigManager
    .dynamicConfigManager()
    .addListener(
      ConfigListener.forConfigList(
        "atlas.aggregator.rollup-policy",
        ps => updateRollupPolicies(ps)
      )
    )

  private def updateRollupPolicies(policies: java.util.List[_ <: Config]): Unit = {
    val idx = QueryIndex.newInstance[RollupPolicy](new NoopRegistry)
    policies.forEach { c =>
      val query = Parser.parseQuery(c.getString("query"))
      val rollup = c.getStringList("rollup")
      val policy = RollupPolicy(query, rollup)
      idx.add(query, policy)
    }
    rollupPolicies = idx
  }

  private val aggrTag = {
    if (config.getBoolean("include-aggr-tag"))
      Tag.of("atlas.aggr", NetflixEnvironment.instanceId())
    else
      Tag.of(TagKey.dsType, "sum")
  }

  private val maxUserTags = config.getInt("validation.max-user-tags")

  private val validator = {
    val rs = Rule.load(config.getConfigList("validation.rules"))
    require(rs.nonEmpty, "validation rule set is empty")
    rs.foreach { r =>
      require(r.isInstanceOf[TagRule], s"only TagRule instances are permitted: $r")
    }
    CompositeTagRule(rs.map(_.asInstanceOf[TagRule]))
  }

  private val allowedCharacters = AsciiSet.fromPattern(config.getString("allowed-characters"))

  private val stringCache = Caffeine
    .newBuilder()
    .maximumSize(config.getInt("cache.strings.max-size"))
    .build[String, String]()

  /**
    * For internal usage after validation has already been performed. Avoids the cost of
    * validation and data cleaning steps that are normally done while reading.
    */
  val internal: PayloadDecoder = new PayloadDecoder(s => s, NoValidation, ts => ts)

  /**
    * Get a default instance for reading/writing the payloads. It will perform full validation
    * and cleanup of data that is read.
    */
  val default: PayloadDecoder = new PayloadDecoder(replaceInvalidCharacters, validator, rollup)

  private def replaceInvalidCharacters(s: String): String = {
    var value = stringCache.getIfPresent(s)
    if (value == null) {
      // To avoid blocking for loading cache, we do an explicit get/put. The replacement may
      // occur multiple times for the same string, but that is not a problem here.
      value = allowedCharacters.replaceNonMembers(s, '_')
      stringCache.put(s, value)
    }
    value
  }

  /**
    * Handle any automatic rollups on the id.
    */
  private def rollup(id: Id): Id = {
    val rollup = new java.util.HashSet[String]()
    rollupPolicies.forEachMatch(id, p => rollup.addAll(p.rollup))
    if (rollup.isEmpty)
      id
    else
      id.filterByKey(k => !rollup.contains(k))
  }
}
