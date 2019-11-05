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
package com.netflix.atlas.aggregator

import java.time.Duration

import javax.inject.Inject
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Tag
import com.netflix.spectator.atlas.AtlasRegistry
import com.netflix.spectator.impl.AsciiSet
import com.typesafe.scalalogging.StrictLogging

class UpdateApi @Inject()(
  evaluator: Evaluator,
  aggrRegistry: AtlasRegistry
) extends WebApi {

  require(aggrRegistry != null, "no binding for aggregate registry")

  import UpdateApi._

  def routes: Route = {
    endpointPath("api" / "v4" / "update") {
      post {
        parseEntity(customJson(p => processPayload(p, aggrRegistry))) { payload =>
          val src = Source.single(ByteString("{}"))
          val entity = HttpEntity(MediaTypes.`application/json`, src)
          complete(HttpResponse(StatusCodes.OK, entity = entity))
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

  private val allowedCharacters = AsciiSet.fromPattern("-._A-Za-z0-9^~")

  private val stringCache = Caffeine
    .newBuilder()
    .maximumSize(2000000)
    .expireAfterAccess(Duration.ofMinutes(10))
    .build[String, String]()

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

  def processPayload(parser: JsonParser, registry: AtlasRegistry): Unit = {
    requireNextToken(parser, JsonToken.START_ARRAY)
    val numStrings = nextInt(parser)
    val strings = loadStringTable(numStrings, parser)

    var t = parser.nextToken()
    while (t != null && t != JsonToken.END_ARRAY) {
      val numTags = parser.getIntValue
      val tags = loadTags(numTags, strings, parser)
      // TODO: validate num tags and lengths
      val op = nextInt(parser)
      val value = nextDouble(parser)
      val id = createId(registry, rollup(tags))
      op match {
        case ADD =>
          // Add the aggr tag to avoid values getting deduped on the backend
          logger.debug(s"received updated, ADD $id $value")
          registry.counter(id.withTag(aggrTag)).add(value)
        case MAX =>
          logger.debug(s"received updated, MAX $id $value")
          registry.maxGauge(id).set(value)
        case unk =>
          throw new IllegalArgumentException(
            s"unknown operation $unk, expected add ($ADD) or max ($MAX)"
          )
      }
      t = parser.nextToken()
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

  private def loadTags(n: Int, strings: Array[String], parser: JsonParser): TagMap = {
    val tags = new SmallHashMap.Builder[String, String](n * 2)
    var i = 0
    while (i < n) {
      val k = strings(nextInt(parser))
      val v = strings(nextInt(parser))
      tags.add(k, v)
      i += 1
    }
    tags.result
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

  private def createId(registry: AtlasRegistry, tags: TagMap): Id = {
    val name = tags("name")
    val otherTags = (tags - "name").asInstanceOf[TagMap]
    registry.createId(name, otherTags.asJavaMap)
  }
}
