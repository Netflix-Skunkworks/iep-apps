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
package com.netflix.atlas.druid

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.apache.pekko.util.Timeout
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.druid.ForeachApi.RewriteEntry
import com.netflix.atlas.json.Json
import com.netflix.atlas.webapi.TagsApi.ListValuesRequest
import com.netflix.atlas.webapi.TagsApi.ValueListResponse
import com.typesafe.config.Config

import scala.concurrent.duration.*

class ForeachApi(config: Config, implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  private val dbRef = actorRefFactory.actorSelection("/user/db")

  private def evalGraph(expr: String): List[StyleExpr] = {
    interpreter.execute(expr).stack.map {
      case ModelExtractors.PresentationType(e) => e
      case v                                   => throw new MatchError(v)
    }
  }

  private def evalQuery(expr: String): Query = {
    interpreter.execute(expr).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalArgumentException(s"invalid query: $expr")
    }
  }

  override def routes: Route = {
    endpointPath("api" / "v1" / "foreach") {
      get {
        parameters("q".as[String], "in".as[String], "k".as[String].*) { (q, in, ks) =>
          val exprs = evalGraph(q)
          val inQuery = evalQuery(in)
          val source = rewrite(RewriteEntry(exprs, inQuery, ks.toList, Map.empty))
            .map(entry => ByteString(Json.encode(entry.toItem)))
            .intersperse(ByteString("["), ByteString(","), ByteString("]"))
          val entity = HttpEntity(MediaTypes.`application/json`, source)
          complete(entity)
        }
      }
    }
  }

  private def tagValues(key: String, query: Query): Source[List[String], NotUsed] = {
    val tq = TagQuery(Some(query), Some(key))
    val future = org.apache.pekko.pattern.ask(dbRef, ListValuesRequest(tq))(Timeout(10.seconds))
    Source
      .future(future)
      .collect {
        case ValueListResponse(vs) => vs
      }
  }

  private def rewrite(entry: RewriteEntry): Source[RewriteEntry, NotUsed] = {
    if (entry.keys.isEmpty) {
      Source.single(entry)
    } else {
      val key = entry.keys.head
      tagValues(key, entry.inQuery)
        .flatMapConcat(Source.apply)
        .flatMapConcat { v =>
          val newInQuery = Query.And(entry.inQuery, Query.Equal(key, v))
          val newExprs = entry.exprs.map { ds =>
            val expr = ds.rewrite {
              case q: Query => if (q == entry.inQuery) newInQuery else q
            }
            expr.asInstanceOf[StyleExpr]
          }
          val newEntry =
            RewriteEntry(newExprs, newInQuery, entry.keys.tail, entry.tags + (key -> v))
          rewrite(newEntry)
        }
    }
  }
}

object ForeachApi {

  case class RewriteEntry(
    exprs: List[StyleExpr],
    inQuery: Query,
    keys: List[String],
    tags: Map[String, String]
  ) {
    def toItem: Item = Item(exprs.mkString(","), tags)
  }

  case class Item(q: String, tags: Map[String, String])
}
