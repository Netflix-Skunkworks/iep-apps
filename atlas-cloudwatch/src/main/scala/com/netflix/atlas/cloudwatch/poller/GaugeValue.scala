package com.netflix.atlas.cloudwatch.poller

import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Statistic

case class GaugeValue(tags: Map[String, String], value: Double) {

  def id: Id = {
    import scala.jdk.CollectionConverters._
    val ts = (tags - "name").map {
      case (k, v) => k -> (if (v.length > 120) "VALUE_TOO_LONG" else v)
    }.asJava
    Id.create(tags("name")).withTags(ts).withTag(Statistic.gauge)
  }
}

object GaugeValue {

  def apply(name: String, value: Double): GaugeValue = {
    apply(Map("name" -> name), value)
  }
}
