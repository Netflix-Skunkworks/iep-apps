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

import java.net.URI
import java.util.regex.Pattern

/**
  * Utilities for determining the region/env scope of an Atlas expression URI,
  * used to validate that an expression targets the correct forwarder instance.
  *
  * Supports two URI styles:
  *
  * Legacy: region/env embedded in the hostname
  *   http://atlas-iep.us-east-1.iepprod.example.com/api/v1/graph?q=...
  *
  * New (atlas-query): region/env carried via query parameters
  *   http://atlas-query.example.com/api/v1/graph?q=...&ns=iep-us-east-1.prod
  *   http://atlas-query.example.com/api/v1/graph?q=...&ns=main.prod&cq=nf.region,us-east-1,:eq
  *   http://atlas-query.example.com/api/v1/graph?q=...nf.region,us-east-1,:eq...&ns=main.prod
  */
object ExpressionScope {

  // AWS region shape: <area>-<direction>-<number>, e.g. us-east-1
  private val awsRegionPattern = Pattern.compile("[a-z]+-[a-z]+-\\d")

  // Matches nf.region,<region>,:eq in Atlas stack language
  private val regionQueryPattern = Pattern.compile("nf\\.region,([^,]+),:eq")

  /**
    * Parses query parameters from a URI string into a Map.
    * Returns only the first value for each key.
    */
  private[cw] def queryParams(atlasUri: String): Map[String, String] = {
    val raw = Option(new URI(atlasUri).getRawQuery).getOrElse("")
    raw
      .split("&")
      .flatMap { param =>
        param.split("=", 2) match {
          case Array(k, v) => Some(k -> v)
          case Array(k)    => Some(k -> "")
          case _           => None
        }
      }
      .toMap
  }

  /**
    * Extracts the region from cq= or q= Atlas stack language params by looking
    * for the nf.region,<region>,:eq pattern.
    */
  private[cw] def regionFromQuery(params: Map[String, String]): Option[String] = {
    val queryStr = params.get("cq").orElse(params.get("q")).getOrElse("")
    val m = regionQueryPattern.matcher(queryStr)
    if (m.find()) Some(m.group(1)) else None
  }

  /**
    * Determines the effective (region, env) scope of a new-style atlas-query URI
    * by inspecting the ns= parameter and falling back to cq=/q= for the region.
    *
    * Returns:
    *   None                      - no ns= param present (legacy URI, use regex filter)
    *   Some((Some(region), env)) - region-scoped expression
    *   Some((None, env))         - global expression (no region found in ns=, cq=, or q=)
    */
  def effectiveRegionEnv(atlasUri: String): Option[(Option[String], String)] = {
    val params = queryParams(atlasUri)
    params.get("ns").flatMap { ns =>
      val dotIdx = ns.lastIndexOf('.')
      if (dotIdx < 0) None
      else {
        val env = ns.substring(dotIdx + 1)
        val prefix = ns.substring(0, dotIdx)
        val m = awsRegionPattern.matcher(prefix)
        val region =
          if (m.find()) Some(m.group())
          else regionFromQuery(params)
        Some((region, env))
      }
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
      case Some((None, _)) =>
        false
      case Some((Some(region), env)) =>
        region == instanceRegion && env == instanceEnv
    }
  }
}
