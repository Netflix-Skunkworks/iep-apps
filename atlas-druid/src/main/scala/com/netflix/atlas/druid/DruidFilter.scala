/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.druid

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Query.KeyQuery
import com.netflix.spectator.impl.AsciiSet

trait DruidFilter

object DruidFilter {

  private val allowedChars = AsciiSet.fromPattern("-._A-Za-z0-9^~ ")

  def sanitize(v: String): String = allowedChars.replaceNonMembers(v, '_')

  def forQuery(query: Query): Option[DruidFilter] = {
    val q = removeDatasourceAndName(query)
    if (q == Query.True) None else Some(toFilter(q))
  }

  def toFilter(query: Query): DruidFilter = {
    query match {
      case Query.True                   => throw new UnsupportedOperationException(":true")
      case Query.False                  => throw new UnsupportedOperationException(":false")
      case Query.Equal(k, v)            => Equal(k, v)
      case Query.In(k, vs)              => In(k, vs)
      case Query.GreaterThan(k, v)      => js(k, ">", v)
      case Query.GreaterThanEqual(k, v) => js(k, ">=", v)
      case Query.LessThan(k, v)         => js(k, "<", v)
      case Query.LessThanEqual(k, v)    => js(k, "<=", v)
      case Query.HasKey(k)              => Not(Equal(k, "")) // druid: empty string is same as null
      case Query.Regex(k, v)            => Regex(k, s"^$v")
      case Query.RegexIgnoreCase(k, v)  => Regex(k, s"(?i)^$v")
      case Query.And(q1, q2)            => And(List(toFilter(q1), toFilter(q2)))
      case Query.Or(q1, q2)             => Or(List(toFilter(q1), toFilter(q2)))
      case Query.Not(q)                 => Not(toFilter(q))
    }
  }

  /**
    * The `nf.datasource` and `name` values should have already been confirmed before trying
    * to create a filter. Those values must be listed as `dataSource` and `searchDimensions`
    * respectively. This removes them by rewriting the query so that they are presumed
    * to be true.
    */
  private def removeDatasourceAndName(query: Query): Query = {
    val newQuery = query.rewrite {
      case kq: KeyQuery if kq.k == "nf.datasource" => Query.True
      case kq: KeyQuery if kq.k == "name"          => Query.True
    }
    Query.simplify(newQuery.asInstanceOf[Query])
  }

  private def js(k: String, op: String, v: String): JavaScript = {
    val sanitizedV = sanitize(v)
    JavaScript(k, s"function(x) { return x $op '$sanitizedV'; }")
  }

  case class Equal(dimension: String, value: String) extends DruidFilter {
    val `type`: String = "selector"
  }

  case class Regex(dimension: String, pattern: String) extends DruidFilter {
    val `type`: String = "regex"
  }

  case class In(dimension: String, values: List[String]) extends DruidFilter {
    val `type`: String = "in"
  }

  case class JavaScript(dimension: String, function: String) extends DruidFilter {
    val `type`: String = "javascript"
  }

  case class And(fields: List[DruidFilter]) extends DruidFilter {
    val `type`: String = "and"
  }

  case class Or(fields: List[DruidFilter]) extends DruidFilter {
    val `type`: String = "or"
  }

  case class Not(field: DruidFilter) extends DruidFilter {
    val `type`: String = "not"
  }
}
