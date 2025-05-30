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
package com.netflix.atlas.stream

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.Json

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

// Parse and validation DataSource's
case class DataSourceValidator(maxDataSourcesPerSession: Int, validateFunc: DataSource => Unit) {

  def validate(input: String): Either[List[IdAndError], DataSources] = {
    val errorMap = mutable.Map[String, mutable.Set[String]]()
    var dataSourceList: List[DataSource] = List.empty[DataSource]
    try {
      dataSourceList = Json.decode[List[DataSource]](input)
    } catch {
      case e: Exception =>
        addError("_", s"failed to parse input: ${e.getMessage}", errorMap)
    }
    validate(dataSourceList, errorMap)
  }

  def validate(dataSourceList: List[DataSource]): Either[List[IdAndError], DataSources] = {
    validate(dataSourceList, mutable.Map.empty)
  }

  private def validate(
    dataSourceList: List[DataSource],
    errorMap: mutable.Map[String, mutable.Set[String]]
  ): Either[List[IdAndError], DataSources] = {

    // Validate size limit first
    if (dataSourceList.size > maxDataSourcesPerSession) {
      addError("_", s"number of DataSources cannot exceed $maxDataSourcesPerSession", errorMap)
    } else {
      // Validate each DataSource
      val visitedIds = mutable.Set[String]()
      dataSourceList.foreach(ds => {
        val id = ds.id()
        // Validate id
        if (id == null) {
          addError(id, "id cannot be null", errorMap)
        } else if (id.isEmpty) {
          addError(id, "id cannot be empty", errorMap)
        } else {
          if (visitedIds.contains(id)) {
            addError(id, "id cannot be duplicated", errorMap)
          } else {
            visitedIds.add(id)
          }
        }
        // Validate uri
        Try(validateFunc(ds)) match {
          case Success(_) =>
          case Failure(e) => addError(id, s"invalid uri: ${e.getMessage}", errorMap)
        }
      })
    }

    if (errorMap.nonEmpty) {
      Left(
        errorMap
          .map { case (id, errorList) => IdAndError(id, errorList.mkString("; ")) }
          .toList
          .sortBy(_.id)
      )
    } else {
      Right(new DataSources(dataSourceList.toSet.asJava))
    }
  }

  private def addError(
    id: String,
    value: String,
    errorMap: mutable.Map[String, mutable.Set[String]]
  ): Unit = {
    val normalizedId =
      if (id == null) {
        "<null_id>"
      } else if (id.isEmpty) {
        "<empty_id>"
      } else {
        id
      }
    errorMap.getOrElseUpdate(normalizedId, mutable.Set[String]()) += value
  }

  case class IdAndError(id: String, error: String)
}
