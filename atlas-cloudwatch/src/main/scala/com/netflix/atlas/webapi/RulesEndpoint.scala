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
package com.netflix.atlas.webapi

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import com.netflix.atlas.pekko.CustomDirectives.endpointPath
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.cloudwatch.CloudWatchRules
import com.netflix.atlas.cloudwatch.MetricCategory
import com.netflix.atlas.cloudwatch.MetricDefinition
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import scala.util.Using

class RulesEndpoint(
  rules: CloudWatchRules
)(implicit val system: ActorSystem)
    extends WebApi
    with StrictLogging {

  override def routes: Route = {
    get {
      endpointPath("api" / "v1" / "rules") {
        handleReq
      }
    }
  }

  private def handleReq: Route = {
    parameters("namespace".optional, "metric".optional) { (ns, m) =>
      val namespace = ns.getOrElse("")
      val metric = m.getOrElse("")
      val result: Map[String, Map[String, List[(MetricCategory, List[MetricDefinition])]]] =
        (namespace.nonEmpty, metric.nonEmpty) match {
          case (true, true) =>
            rules.rules
              .filter(t => t._1.equals(namespace))
              .map(t => t._1 -> t._2.filter(i => i._1.equals(metric)))
          case (true, _) => rules.rules.filter(t => t._1.equals(namespace))
          case (_, _)    => rules.rules
        }
      complete(StatusCodes.OK, HttpEntity(ContentTypes.`application/json`, encode(result)))
    }
  }

  private def encode(
    result: Map[String, Map[String, List[(MetricCategory, List[MetricDefinition])]]]
  ): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    Using.resource(Json.newJsonGenerator(stream)) { json =>
      json.writeStartObject()
      result.foreachEntry { (ns, metrics) =>
        json.writeStringField("ns", ns)
        json.writeObjectFieldStart("metrics")

        metrics.foreachEntry { (metric, list) =>
          json.writeArrayFieldStart("categories")
          list.foreach { tuple =>
            val (category, definitions) = tuple
            json.writeObjectFieldStart(metric)
            json.writeNumberField("period", category.period)
            json.writeNumberField("graceOverride", category.graceOverride)

            json.writeArrayFieldStart("dimensions")
            category.dimensions.foreach(json.writeString(_))
            json.writeEndArray()

            if (category.filter.isDefined) {
              json.writeStringField("filter", category.filter.get.toString)
            }

            json.writeArrayFieldStart("definitions")
            definitions.foreach { d =>
              json.writeStartObject()
              json.writeStringField("alias", d.alias)
              if (d.monotonicValue) json.writeBooleanField("monotonicValue", true)
              json.writeObjectFieldStart("tags")
              d.tags.foreachEntry((k, v) => json.writeStringField(k, v))
              json.writeEndObject()
              json.writeEndObject()
            }
            json.writeEndArray()
            json.writeEndObject()
          }
          json.writeEndArray()
        }
        json.writeEndObject()
      }

      json.writeEndObject()
    }
    stream.toByteArray
  }
}
