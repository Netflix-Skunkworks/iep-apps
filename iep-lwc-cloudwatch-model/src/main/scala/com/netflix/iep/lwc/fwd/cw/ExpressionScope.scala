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
package com.netflix.iep.lwc.fwd.cw

import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.ModelDataTypes
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.uri.QueryParam
import com.netflix.atlas.core.uri.UriParser
import com.netflix.iep.config.ConfigManager

import java.util.regex.Pattern
import scala.util.control.NonFatal

/**
  * Scope of an Atlas expression URI.
  *
  * @param env    deployment environment the expression targets, e.g. `test` or `prod`
  * @param region AWS region the expression is scoped to, or `None` if the expression has
  *               no single region (global, or spanning multiple regions)
  */
case class ExpressionScope(env: String, region: Option[String])

/**
  * Utilities for determining the region/env scope of an Atlas expression URI,
  * used to validate that an expression targets the correct forwarder instance.
  *
  * Supports two URI styles:
  *
  * Legacy: region/env embedded in the hostname. [[effectiveRegionEnv]] parses them from
  * the host directly; [[matchesInstance]] additionally accepts the configured filter regex.
  *
  * New (atlas-query): region/env carried via query parameters. Env comes from the ns=
  * namespace suffix; region is resolved from the ns= namespace prefix, then the cq=/q=
  * query expressions. Global expressions (no region from any source) are not supported.
  */
object ExpressionScope {

  // Interpreter used to parse the q=/cq= Atlas stack language expressions
  private val interpreter = Interpreter(new CustomVocabulary(ConfigManager.get()).allWords)

  // AWS region shape, e.g. us-east-1 / ap-southeast-2. Shared by the namespace and host
  // patterns so a hyphenated service name (e.g. "api-service") is not mistaken for a region.
  private val RegionShape = "[a-z]+-[a-z]+-\\d+"

  // Matches a region-scoped namespace, e.g. main-us-east-1.test, capturing (region, env).
  // The region segment must look like an AWS region; a global namespace such as main.test or a
  // hyphenated service name such as api-service.prod has no region segment and does not match.
  private val RegionSpecificNsPattern = raw"""^[^.]+-($RegionShape)\.(test|prod)$$""".r

  // Matches a legacy host where region/env are embedded in the hostname, e.g.
  // atlas-foo.us-east-1.test.example.com, capturing (region, env).
  private val RegionSpecificHostPattern =
    raw"""^[^.]+\.($RegionShape)\.(?:[a-z]+)?(test|prod)\..*$$""".r

  /**
    * Determines the effective region/env scope of an expression URI.
    *
    * For a new-style atlas-query URI (has an ns= param) the env comes from the namespace
    * suffix, and the region is resolved from the namespace prefix first, then from the
    * cq=/q= query expressions. For a legacy URI (no ns= param) the region/env are parsed
    * from the hostname.
    *
    * Returns:
    *   None                                       - URI carries no recognizable scope
    *   Some(ExpressionScope(env, Some(region)))   - region-scoped expression
    *   Some(ExpressionScope(env, None))           - global expression, or one spanning
    *                                                multiple regions (no single region)
    */
  def effectiveRegionEnv(atlasUri: String): Option[ExpressionScope] = {
    val uri = UriParser.parse(atlasUri)
    val params = uri.query
    params.find(_.key.text == "ns") match {
      case Some(ns) =>
        val env = extractEnv(ns.decodedValue)
        extractFromRegionalNamespace(ns.decodedValue).orElse {
          extractFromQuery(env, params)
        }
      case None =>
        uri.host.flatMap(h => extractFromHost(h.text))
    }
  }

  // Env is the namespace suffix; anything that is not explicitly prod is treated as test.
  private def extractEnv(ns: String): String = {
    if (ns.endsWith(".prod")) "prod" else "test"
  }

  // Region from a region-scoped namespace prefix, e.g. main-us-east-1.test -> us-east-1.
  private def extractFromRegionalNamespace(ns: String): Option[ExpressionScope] = {
    ns match {
      case RegionSpecificNsPattern(region, env) => Some(ExpressionScope(env, Some(region)))
      case _                                    => None
    }
  }

  /**
    * Resolves the region from the query expressions when it is not encoded in the namespace.
    *
    * The cq= common query (if present) is AND'd into each q= expression, then the nf.region
    * tag is extracted from every resulting expression. A region is only returned when all
    * expressions agree on exactly one region; no region or multiple distinct regions yields
    * `None` (treated as global / unknown scope).
    */
  private def extractFromQuery(env: String, params: List[QueryParam]): Option[ExpressionScope] = {
    params.find(_.key.text == "q").map { q =>
      val exprs = parseGraphExpr(q.decodedValue)
      val cq = params.find(_.key.text == "cq").flatMap(p => parseCqExpr(p.decodedValue))
      val finalExprs = applyCq(exprs, cq)
      val regions = finalExprs.flatMap(expr => regionFromExpr(expr)).distinct
      val region = regions match {
        case r :: Nil => Some(r)
        case _        => None
      }
      ExpressionScope(env, region)
    }
  }

  // Interpret a q= value and keep the presentation (style) expressions left on the stack.
  // A malformed expression cannot be scoped to a region, so treat a parse failure as "no
  // expressions" rather than letting the interpreter throw into the forwarder hot path.
  private def parseGraphExpr(expr: String): List[StyleExpr] = {
    try {
      interpreter.execute(expr).stack.collect {
        case ModelDataTypes.PresentationType(t) => t
      }
    } catch {
      case NonFatal(_) => Nil
    }
  }

  // A cq= value must interpret to exactly one query; anything else (or a parse failure) is ignored.
  private def parseCqExpr(expr: String): Option[Query] = {
    try {
      interpreter.execute(expr).stack match {
        case (q: Query) :: Nil => Some(q)
        case _                 => None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  // AND the common query into every expression; a no-op when there is no cq=.
  private def applyCq(exprs: List[StyleExpr], cq: Option[Query]): List[StyleExpr] = {
    cq.fold(exprs) { q =>
      exprs.map(expr => applyCq(expr, q))
    }
  }

  private def applyCq(expr: StyleExpr, cq: Query): StyleExpr = {
    expr
      .rewrite {
        case q: Query => q.and(cq)
      }
      .asInstanceOf[StyleExpr]
  }

  // Region for a single expression. Query.tags only returns tags constrained to a single
  // value via :eq, so an :in or :or over multiple regions contributes no nf.region tag and
  // an expression spanning several regions resolves to None (unknown).
  private def regionFromExpr(expr: StyleExpr): Option[String] = {
    val regions = expr.expr.dataExprs.flatMap { de =>
      Query.tags(de.query).get("nf.region")
    }.distinct

    // Multiple regions or no region maps to `None` to represent unknown
    regions match {
      case r :: Nil => Some(r)
      case _        => None
    }
  }

  // Region/env from a legacy hostname; None if the host does not carry them.
  private def extractFromHost(host: String): Option[ExpressionScope] = {
    host match {
      case RegionSpecificHostPattern(region, env) => Some(ExpressionScope(env, Some(region)))
      case _                                      => None
    }
  }

  /**
    * Returns true if the expression should be processed by a forwarder instance
    * with the given region and env.
    *
    * Falls back to the legacy regex for old-style URIs (region/env in hostname).
    * Global expressions (ns= with no region scope) are not supported and are rejected.
    */
  def matchesInstance(
    atlasUri: String,
    legacyPattern: Pattern,
    instanceRegion: String,
    instanceEnv: String
  ): Boolean = {
    if (legacyPattern.matcher(atlasUri).matches()) return true
    effectiveRegionEnv(atlasUri) match {
      case None =>
        false
      case Some(ExpressionScope(_, None)) =>
        false
      case Some(ExpressionScope(env, Some(region))) =>
        region == instanceRegion && env == instanceEnv
    }
  }
}
