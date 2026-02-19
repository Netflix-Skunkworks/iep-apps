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
package com.netflix.iep.lwc.fwd.admin

import com.netflix.atlas.json3.Json
import com.networknt.schema.Schema
import com.networknt.schema.SchemaRegistry
import com.networknt.schema.SpecificationVersion
import com.typesafe.scalalogging.StrictLogging
import tools.jackson.databind.JsonNode

import scala.io.Source
import scala.jdk.CollectionConverters.*

class SchemaValidation extends StrictLogging {

  val schema: Schema = {
    val reader = Source.fromResource("cw-fwding-cfg-schema.json").reader()
    try {
      val tree = Json.decode[JsonNode](reader)
      val registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_4)
      val schema = registry.getSchema(tree.get("schema"))
      schema.initializeValidators()
      schema
    } finally {
      reader.close()
    }
  }

  def validate(json: JsonNode): Unit = {
    val errors =
      schema.validate(json, ctx => ctx.executionConfig(cfg => cfg.formatAssertionsEnabled(true)))
    if (!errors.isEmpty) {
      throw new IllegalArgumentException(
        errors.asScala.map(_.getMessage).mkString("\n")
      )
    }
  }

}
