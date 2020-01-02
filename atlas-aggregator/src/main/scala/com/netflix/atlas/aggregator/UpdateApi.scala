/*
 * Copyright 2014-2020 Netflix, Inc.
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

import javax.inject.Inject
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.validation.CompositeTagRule
import com.netflix.atlas.core.validation.Rule
import com.netflix.atlas.core.validation.TagRule
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.iep.NetflixEnvironment
import com.netflix.iep.config.ConfigManager
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Tag
import com.netflix.spectator.impl.AsciiSet
import com.typesafe.scalalogging.StrictLogging

class UpdateApi @Inject()(
  evaluator: Evaluator,
  aggrService: AtlasAggregatorService
) extends WebApi {

  require(aggrService != null, "no binding for aggregate registry")

  import UpdateApi._

  def routes: Route = {
    endpointPath("api" / "v4" / "update") {
      post {
        parseEntity(customJson(p => processPayload(p, aggrService))) { response =>
          complete(response)
        }
      }
    }
  }
}

object UpdateApi extends StrictLogging {

  import com.netflix.atlas.json.JsonParserHelper._

  type TagMap = SmallHashMap[String, String]

  private val ADD = 0
  private val MAX = 10

  private val aggrTag = Tag.of("atlas.aggr", NetflixEnvironment.instanceId())

  private val config = ConfigManager.get().getConfig("atlas.aggregator")

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
    .expireAfterAccess(config.getDuration("cache.strings.expires-after"))
    .build[String, String]()

  private val idCache = Caffeine
    .newBuilder()
    .maximumSize(config.getInt("cache.ids.max-size"))
    .expireAfterAccess(config.getDuration("cache.ids.expires-after"))
    .build[TagMap, Id]()

  def processPayload(parser: JsonParser, service: AtlasAggregatorService): HttpResponse = {
    val validationResults = List.newBuilder[ValidationResult]
    var numDatapoints = 0

    requireNextToken(parser, JsonToken.START_ARRAY)
    val numStrings = nextInt(parser)
    val strings = loadStringTable(numStrings, parser)

    var t = parser.nextToken()
    while (t != null && t != JsonToken.END_ARRAY) {
      numDatapoints += 1
      val numTags = parser.getIntValue
      val result = loadTags(numTags, strings, parser)
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
              service.add(id.withTag(aggrTag), value)
            case MAX =>
              logger.debug(s"received updated, MAX $id $value")
              service.max(id, value)
            case unk =>
              throw new IllegalArgumentException(
                s"unknown operation $unk, expected add ($ADD) or max ($MAX)"
              )
          }
      }
      t = parser.nextToken()
    }
    createResponse(numDatapoints, validationResults.result())
  }

  private val okResponse = {
    val entity = HttpEntity(MediaTypes.`application/json`, "{}")
    HttpResponse(StatusCodes.OK, entity = entity)
  }

  private def createErrorResponse(status: StatusCode, msg: FailureMessage): HttpResponse = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    HttpResponse(status, entity = entity)
  }

  private def createResponse(numDatapoints: Int, failures: List[ValidationResult]): HttpResponse = {
    if (failures.isEmpty) {
      okResponse
    } else {
      val numFailures = failures.size
      if (numDatapoints > numFailures) {
        // Partial failure
        val msg = FailureMessage.partial(failures, numFailures)
        createErrorResponse(StatusCodes.Accepted, msg)
      } else {
        // All datapoints dropped
        val msg = FailureMessage.error(failures, numFailures)
        createErrorResponse(StatusCodes.BadRequest, msg)
      }
    }
  }

  private def loadStringTable(n: Int, parser: JsonParser): Array[String] = {
    val strings = new Array[String](n)
    var i = 0
    while (i < n) {
      strings(i) = replaceInvalidCharacters(nextString(parser))
      i += 1
    }
    strings
  }

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

  private def loadTags(
    n: Int,
    strings: Array[String],
    parser: JsonParser
  ): Either[ValidationResult, Id] = {
    var result: ValidationResult = ValidationResult.Pass
    var numUserTags = 0
    val tags = new SmallHashMap.Builder[String, String](n * 2)
    var i = 0
    while (i < n) {
      val k = strings(nextInt(parser))
      val v = strings(nextInt(parser))
      if (result == ValidationResult.Pass) {
        // Avoid doing unnecessary work if it has already failed validation. We still need
        // to process the entry as subsequent entries may be fine.
        tags.add(k, v)
        if (!TagKey.isRestricted(k)) {
          numUserTags += 1
        }
        result = validator.validate(k, v)
      }
      i += 1
    }

    if (result == ValidationResult.Pass) {
      // Validation of individual key/value pairs passed, now check rules for the
      // overall tag set
      if (numUserTags > maxUserTags) {
        Left(
          ValidationResult
            .Fail("MaxUserTagsRule", s"too many user tags: $numUserTags > $maxUserTags")
        )
      } else {
        val ts = tags.result
        if (ts.contains("name"))
          Right(convertToId(rollup(ts)))
        else
          Left(ValidationResult.Fail("HasKeyRule", s"missing 'name': ${ts.keys}"))
      }
    } else {
      // Tag rule check failed
      Left(result)
    }
  }

  private def convertToId(tags: TagMap): Id = {
    var value = idCache.getIfPresent(tags)
    if (value == null) {
      // To avoid blocking for loading cache, we do an explicit get/put. The replacement may
      // occur multiple times for the same id, but that is not a problem here.
      value = createId(tags)
      idCache.put(tags, value)
    }
    value
  }

  /**
    * Handle any automatic rollups on the id. For now it just removes the node dimension for
    * ids with percentiles.
    */
  private def rollup(tags: TagMap): TagMap = {
    if (tags.contains("percentile"))
      (tags - "nf.node" - "nf.task").asInstanceOf[TagMap]
    else
      tags
  }

  private def createId(tags: TagMap): Id = {
    val name = tags("name")
    val otherTags = (tags - "name").asInstanceOf[TagMap]
    Id.create(name).withTags(otherTags.asJavaMap)
  }
}
