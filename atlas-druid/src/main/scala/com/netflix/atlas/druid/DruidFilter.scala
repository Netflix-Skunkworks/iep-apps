/*
 * Copyright 2014-2018 Netflix, Inc.
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
import com.netflix.spectator.impl.AsciiSet

trait DruidFilter

object DruidFilter {

  private val allowedChars = AsciiSet.fromPattern("-._A-Za-z0-9^~ ")

  def sanitize(v: String): String = allowedChars.replaceNonMembers(v, '_')

  def forQuery(query: Query): Option[DruidFilter] = {
    query match {
      case Query.True                   => None
      case Query.False                  => throw new UnsupportedOperationException(":false")
      case Query.Equal(k, v)            => Some(Equal(k, v))
      case Query.In(k, vs)              => Some(In(k, vs))
      case Query.GreaterThan(k, v)      => Some(js(k, ">", v))
      case Query.GreaterThanEqual(k, v) => Some(js(k, ">=", v))
      case Query.LessThan(k, v)         => Some(js(k, "<", v))
      case Query.LessThanEqual(k, v)    => Some(js(k, "<=", v))
      case Query.HasKey(k)              => Some(JavaScript(k, "function(x) { return true; }"))
      case Query.Regex(k, v)            => Some(Regex(k, s"^$v"))
      case Query.RegexIgnoreCase(k, v)  => throw new UnsupportedOperationException(":reic")
      case Query.And(q1, q2)            => Some(And(List(forQuery(q1).get, forQuery(q2).get)))
      case Query.Or(q1, q2)             => Some(Or(List(forQuery(q1).get, forQuery(q2).get)))
      case Query.Not(q)                 => Some(Not(forQuery(q).get))
    }
  }

  private def js(k: String, op: String, v: String): JavaScript = {
    val sanitizedV = sanitize(v)
    JavaScript(k, s"function(x) { return x $op '$sanitizedV'; }")
  }

  case class Equal(dimension: String, value: String) extends DruidFilter {
    val `type`: String = "selector"
  }

  case class Regex(dimension: String, value: String) extends DruidFilter {
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
