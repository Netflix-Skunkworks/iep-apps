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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.core.model.Datapoint
import com.netflix.spectator.impl.AsciiSet
import com.typesafe.config.Config
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import scala.util.matching.Regex

/**
  * Tag datapoints with a set of common tags. Also allows mapping cloudwatch
  * dimension names to names that match some other convention. For example,
  * the cloudwatch dimension name AutoScalingGroupName corresponds to nf.asg
  * and we would rather have a single key in use for common concepts.
  */
class DefaultTagger(config: Config) extends Tagger {

  import scala.jdk.CollectionConverters.*

  private val validTagChars = AsciiSet.fromPattern(config.getString("valid-tag-characters"))

  private val validTagValueChars = {
    import scala.jdk.CollectionConverters.*
    config
      .getConfigList("valid-tag-value-characters")
      .asScala
      .map { cfg =>
        cfg.getString("key") -> AsciiSet.fromPattern(cfg.getString("value"))
      }
      .toMap
  }

  private val extractors: Map[String, List[(Regex, String)]] = config
    .getConfigList("extractors")
    .asScala
    .map { c =>
      val directives = c
        .getConfigList("directives")
        .asScala
        .map { cl =>
          val alias = if (cl.hasPath("alias")) cl.getString("alias") else c.getString("name")
          cl.getString("pattern").r -> alias
        }
        .toList
      c.getString("name") -> directives
    }
    .toMap

  private val mappings: Map[String, String] = config
    .getConfigList("mappings")
    .asScala
    .map(c => c.getString("name") -> c.getString("alias"))
    .toMap

  private val commonTags: Map[String, String] = config
    .getConfigList("common-tags")
    .asScala
    .map(c => c.getString("key") -> c.getString("value"))
    .toMap

  private def toTag(dimension: Dimension): (String, String) = {
    val cwName = dimension.name
    val cwValue = dimension.value

    val extractor = DefaultTagger.ValueExtractor(cwValue)
    extractors
      .get(cwName)
      .flatMap { directives =>
        directives.collectFirst {
          case extractor(a, v) => a -> v
        }
      }
      .getOrElse(mappings.getOrElse(cwName, cwName) -> cwValue)
  }

  override def fixTags(d: Datapoint): Datapoint = {
    val tags = d.tags.map {
      case (k, v) =>
        val nk = validTagChars.replaceNonMembers(k, '_')
        val nv = validTagValueChars.getOrElse(nk, validTagChars).replaceNonMembers(v, '_')
        nk -> nv
    }
    d.copy(tags = tags)
  }

  override def apply(dimensions: List[Dimension]): Map[String, String] = {
    commonTags ++ dimensions.map(toTag).toMap
  }
}

object DefaultTagger {

  private case class ValueExtractor(rawValue: String) {

    def unapply(extractorDirective: (Regex, String)): Option[(String, String)] = {
      val (regex, alias) = extractorDirective
      regex.findFirstMatchIn(rawValue).map { matches =>
        val value = if (matches.groupCount > 0) matches.subgroups.mkString("-") else rawValue
        alias -> value
      }
    }
  }
}
