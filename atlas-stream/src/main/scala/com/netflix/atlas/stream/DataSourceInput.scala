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
package com.netflix.atlas.stream

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.Json
import com.netflix.atlas.stream.DataSourceInput.IdAndError
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

// Parse and validation DataSource's
class DataSourceInput private (validateFunc: DataSource => Unit) extends StrictLogging {

  private val errorMap = mutable.Map[String, mutable.Set[String]]()
  private var dataSourceList: List[DataSource] = List.empty[DataSource]

  def this(input: String, validateFunc: DataSource => Unit) {
    this(validateFunc)
    parse(input)
    validate()
  }

  def this(dataSourceList: List[DataSource], validateFunc: DataSource => Unit) {
    this(validateFunc)
    this.dataSourceList = dataSourceList
    validate()
  }

  def isValid: Boolean = errorMap.isEmpty

  def dataSources: DataSources = {
    if (isValid) {
      new DataSources(dataSourceList.toSet.asJava)
    } else {
      null
    }
  }

  // Validation errors as a map, key is the DataSource id, id "_" means it's a general error
  def errors: List[IdAndError] = {
    errorMap
      .map {
        case (id, errorList) => IdAndError(id, errorList.mkString("; "))
      }
      .toList
      .sortBy(_.id)
  }

  private def parse(input: String): Unit = {
    // Parse json
    try {
      dataSourceList = Json.decode[List[DataSource]](input)
    } catch {
      case e: Exception =>
        addError("_", s"failed to parse input as List[DataSource]: ${e.getMessage}")
    }
  }

  private def validate(): Unit = {
    // Validate each DataSource
    val visitedIds = mutable.Set[String]()
    dataSourceList.foreach(ds => {
      val id = ds.getId

      // Validate id
      if (id == null) {
        addError(id, "id cannot be null")
      } else if (id.isEmpty) {
        addError(id, "id cannot be empty")
      } else {
        if (visitedIds.contains(id)) {
          addError(id, "id cannot be duplicated")
        } else {
          visitedIds.add(id)
        }
      }

      // Validate uri
      Try(validateFunc(ds)) match {
        case Success(_) =>
        case Failure(e) => addError(id, s"invalid uri: ${e.getMessage}")
      }
    })
  }

  private def addError(id: String, value: String): Unit = {
    // Normalize id for null and empty
    val idNormalized =
      if (id == null) {
        "<null_id>"
      } else if (id.isEmpty) {
        "<empty_id>"
      } else {
        id
      }
    errorMap.getOrElseUpdate(idNormalized, mutable.Set[String]()) += value
  }
}

object DataSourceInput {
  case class IdAndError(id: String, error: String)
}
