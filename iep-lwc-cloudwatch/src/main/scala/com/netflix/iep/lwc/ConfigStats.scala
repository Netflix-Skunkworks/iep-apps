package com.netflix.iep.lwc

import com.netflix.iep.lwc.fwd.cw.ClusterConfig

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable.ListMap
import scala.collection.immutable.SortedMap

class ConfigStats {

  import ConfigStats.*
  import scala.jdk.CollectionConverters.*

  // cluster -> atlasUri -> UriInfo
  private val stats = new ConcurrentHashMap[String, ConcurrentHashMap[String, UriInfo]]()

  /** Sync stats information with the current set of configs. */
  def updateConfigs(configs: Map[String, ClusterConfig]): Unit = {
    removeMissingKeys(stats, configs.keySet)
    configs.foreachEntry { (cluster, config) =>
      val configInfo = stats.computeIfAbsent(cluster, _ => new ConcurrentHashMap[String, UriInfo]())
      removeMissingKeys(configInfo, config.expressions.map(_.atlasUri).toSet)
      config.expressions.foreach { expr =>
        configInfo.computeIfAbsent(expr.atlasUri, _ => UriInfo())
      }
    }
  }

  private def removeMissingKeys(map: ConcurrentHashMap[String, ?], keys: Set[String]): Unit = {
    val currentKeys = map.keySet().asScala
    val removedKeys = currentKeys.diff(keys)
    removedKeys.foreach(map.remove)
  }

  /** Update stats for a given uri. */
  def updateStats(cluster: String, uri: String, timestamp: Long): Unit = {
    val configInfo = stats.get(cluster)
    if (configInfo != null) {
      val uriInfo = configInfo.get(uri)
      if (uriInfo != null) {
        uriInfo.timestamp.set(timestamp)
        uriInfo.datapoints.incrementAndGet()
      }
    }
  }

  /** Return a snapshot of the stats. */
  def snapshot: Map[String, List[Map[String, Any]]] = {
    val snapshot = stats
      .entrySet()
      .asScala
      .toList
      .map { entry =>
        val cluster = entry.getKey
        val configInfo = entry.getValue
        cluster -> snapshotConfigInfo(configInfo)
      }
    SortedMap.from(snapshot)
  }

  def snapshotForCluster(cluster: String): List[Map[String, Any]] = {
    val configInfo = stats.get(cluster)
    if (configInfo == null)
      Nil
    else
      snapshotConfigInfo(configInfo)
  }

  private def snapshotConfigInfo(configInfo: ConcurrentHashMap[String, UriInfo]): List[Map[String, Any]] = {
    configInfo
      .entrySet()
      .asScala
      .toList
      .sortWith(_.getKey < _.getKey)
      .map { entry =>
        val uri = entry.getKey
        val info = entry.getValue
        ListMap(
          "uri" -> uri,
          "timestamp" -> Instant.ofEpochMilli(info.timestamp.get()).toString,
          "datapoints" -> info.datapoints.get()
        )
      }
  }

  /** Number of expressions that haven't received an update in the last 5m. */
  def staleExpressions: Int = {
    stats.values().asScala.map(_.values().asScala.count(_.isStale)).sum
  }
}

object ConfigStats {

  case class UriInfo(
    timestamp: AtomicLong = new AtomicLong(System.currentTimeMillis()),
    datapoints: AtomicLong = new AtomicLong()
  ) {

    def isStale: Boolean = {
      System.currentTimeMillis() - timestamp.get() > 300_000
    }
  }
}
