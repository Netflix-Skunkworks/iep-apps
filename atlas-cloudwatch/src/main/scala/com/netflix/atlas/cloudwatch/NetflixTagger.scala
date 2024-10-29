/*
 * Copyright 2014-2024 Netflix, Inc.
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

import com.netflix.frigga.Names
import com.typesafe.config.Config
import software.amazon.awssdk.services.cloudwatch.model.Dimension

/**
  * Tag the datapoints using Frigga to extract app and cluster information based
  * on naming conventions used by Spinnaker and Asgard.
  */
class NetflixTagger(config: Config) extends DefaultTagger(config) {

  import scala.jdk.CollectionConverters.*

  private val keys = config.getStringList("netflix-keys").asScala

  private def opt(k: String, s: String): Option[(String, String)] = {
    Option(s).filter(_ != "").map(v => k -> v)
  }

  override def apply(dimensions: List[Dimension]): Map[String, String] = {
    val baseTags = super.apply(dimensions)
    val resultTags = scala.collection.mutable.Map[String, String]()

    keys.foreach { k =>
      baseTags.get(k) match {
        case Some(v) =>
          val name = Names.parseName(v)
          resultTags ++= List(
            opt("nf.app", name.getApp),
            opt("nf.cluster", name.getCluster),
            opt("nf.stack", name.getStack)
          ).flatten
        case None =>
          // Only add default values if the keys are not already set
          if (!resultTags.contains("nf.app")) resultTags("nf.app") = "cloudwatch"
          if (!resultTags.contains("nf.cluster")) resultTags("nf.cluster") = "cloudwatch"
      }
    }

    // Merge resultTags with baseTags, giving priority to resultTags
    resultTags.toMap ++ baseTags
  }
}
