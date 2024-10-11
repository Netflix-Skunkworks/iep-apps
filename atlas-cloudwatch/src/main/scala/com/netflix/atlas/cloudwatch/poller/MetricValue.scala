package com.netflix.atlas.cloudwatch.poller

import com.netflix.spectator.api.Id

sealed trait MetricValue {
  def id: Id
}

object MetricValue {

  // Utility function to create an Id with tags
  def createId(name: String, tags: Map[String, String]): Id = {
    import scala.jdk.CollectionConverters._
    val ts = (tags - "name").map {
      case (k, v) => k -> (if (v.length > 120) "VALUE_TOO_LONG" else v)
    }.asJava
    Id.create(name).withTags(ts)
  }

  def apply(name: String, value: Double): MetricValue = {
    DoubleValue(Map("name" -> name), value)
  }

  def apply(name: String, value: Long): MetricValue = {
    LongValue(Map("name" -> name), value)
  }
}

// Case class for Double values
case class DoubleValue(tags: Map[String, String], value: Double) extends MetricValue {
  def id: Id = MetricValue.createId(tags("name"), tags)
}

// Case class for Long values
case class LongValue(tags: Map[String, String], value: Long) extends MetricValue {
  def id: Id = MetricValue.createId(tags("name"), tags)
}
