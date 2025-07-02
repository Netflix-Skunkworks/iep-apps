/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.iep.lwc.fwd.admin

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchema
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.io.Source
import scala.jdk.CollectionConverters.*

class SchemaValidation extends StrictLogging {

  val schema: JsonSchema = {
    val reader = Source.fromResource("cw-fwding-cfg-schema.json").reader()
    try {
      JsonSchemaFactory
        .byDefault()
        .getJsonSchema(Json.decode[SchemaCfg](reader).schema)
    } finally {
      reader.close()
    }
  }

  def validate(json: JsonNode): Unit = {
    val pr = schema.validate(json)
    if (!pr.isSuccess) {
      throw new IllegalArgumentException(
        pr.asScala.map(_.getMessage).mkString("\n")
      )
    }
  }

}

case class SchemaCfg(schema: JsonNode, validationHook: String)
