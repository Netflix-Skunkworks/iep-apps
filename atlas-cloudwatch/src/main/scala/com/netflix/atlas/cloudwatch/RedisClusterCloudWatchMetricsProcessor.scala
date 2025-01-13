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
package com.netflix.atlas.cloudwatch

import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.merge
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newCacheEntry
import com.netflix.atlas.cloudwatch.RedisClusterCloudWatchMetricsProcessor.getHash
import com.netflix.atlas.cloudwatch.RedisClusterCloudWatchMetricsProcessor.getKey
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.springframework.beans.factory.annotation.Qualifier
import redis.clients.jedis.CommandObjects
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.Protocol.Command
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.util.JedisClusterCRC16

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.*
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

/**
  * A Redis cluster implementation of the Cloud Watch Metrics processor.
  *
  * Keys are distributed across the cluster based on the XX hash of the AWS data. Scraping works by finding the leaders
  * foreach scrape, scanning all of the keys, and creating a map of key to slots. Each slot is then fetched in a batch call
  * to consolidate the reads.
  *
  * TODO - Note that this means we have to collect all of the keys by slot ID. MGETs do not work across slot boundaries.
  * If there are fewer than 16k entries in a cluster, we'll be making about 16k batch calls. If the map grows to a size
  * that starts to impact performance, we can look at issuing an MGET once some threshold is reached to remove some keys
  * from memory.
  */
class RedisClusterCloudWatchMetricsProcessor(
  config: Config,
  registry: Registry,
  tagger: Tagger,
  @Qualifier("jedisClient") jedis: JedisCluster,
  @Qualifier("valkeyJedisClient") valkeyJedis: JedisCluster,
  leaderStatus: LeaderStatus,
  rules: CloudWatchRules,
  publishRouter: PublishRouter,
  debugger: CloudWatchDebugger
)(override implicit val system: ActorSystem)
    extends CloudWatchMetricsProcessor(config, registry, rules, tagger, publishRouter, debugger) {

  private implicit val executionContext: ExecutionContext =
    system.dispatchers.lookup("redis-io-dispatcher")

  private val updatesNew = registry.counter("atlas.cloudwatch.redis.updates", "id", "new")
  private val updatesExisting = registry.counter("atlas.cloudwatch.redis.updates", "id", "existing")
  private val updateExceptions = registry.createId("atlas.cloudwatch.redis.updateExceptions")

  private val scrapeFailure = registry.counter("atlas.cloudwatch.redis.scrapeFailures")
  private val batchFailure = registry.createId("atlas.cloudwatch.redis.batchFailures")
  private val scanFailure = registry.createId("atlas.cloudwatch.redis.scanFailure")
  private val readExs = registry.createId("atlas.cloudwatch.redis.readExceptions")
  private val writeExs = registry.createId("atlas.cloudwatch.redis.writesExceptions")
  private val batchCalls = registry.createId("atlas.cloudwatch.redis.batchCalls")
  private val keysRead = registry.createId("atlas.cloudwatch.redis.keysRead")
  private val deletes = registry.counter("atlas.cloudwatch.redis.deletes")
  private val deleteFailures = registry.counter("atlas.cloudwatch.redis.deleteFailures")
  private val entriesScraped = registry.createId("atlas.cloudwatch.redis.entriesScraped")
  private[cloudwatch] val casCounter = registry.counter("atlas.cloudwatch.redis.cas")
  private[cloudwatch] val casFailure = registry.counter("atlas.cloudwatch.redis.cas.failure")

  private val scrapeTime = registry.timer("atlas.cloudwatch.redis.scrapeTime")
  private val keysPerBatch = registry.distributionSummary("atlas.cloudwatch.redis.keysPerBatch")

  private val scanParams = new ScanParams()
    .`match`("cwf:*")
    .count(config.getInt("atlas.redis-cluster.scan.count"))
  private val commandObjects = new CommandObjects()
  private val maxBatch = config.getInt("atlas.redis-cluster.batch.size")
  private val pollKey = "cw_last_poll_"

  private val currentScan = new AtomicReference[Future[NotUsed]]()
  private val running = new AtomicBoolean(false)
  private val pubCounter = new AtomicInteger()

  override def updateCache(
    datapoint: FirehoseMetric,
    category: MetricCategory,
    receivedTimestamp: Long
  ): Unit = {
    try {
      val hash = datapoint.xxHash
      val key = getKey(hash)
      val existingJedis = getFromRedis(jedis, key)
      val existingValkeyJedis = getFromRedis(valkeyJedis, key)

      val existing = if (existingJedis != null) existingJedis else existingValkeyJedis

      var isNew: Boolean = false
      val cacheEntry = if (existing == null || existing.isEmpty) {
        isNew = true
        newCacheEntry(datapoint, category, receivedTimestamp)
      } else {
        insertDatapoint(existing, datapoint, category, receivedTimestamp)
      }

      try {
        cas(existing, cacheEntry, key, expSeconds(category.period))
        if (isNew) updatesNew.increment() else updatesExisting.increment()
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to set key in Redis", ex)
          registry
            .counter(writeExs.withTags("call", "set", "ex", ex.getClass.getSimpleName))
            .increment()
      }
    } catch {
      case ex: Exception =>
        logger.error("Unexpected exception on update", ex)
        registry.counter(updateExceptions.withTag("ex", ex.getClass.getSimpleName)).increment()
    }
  }

  private def getFromRedis(client: JedisCluster, key: Array[Byte]): Array[Byte] = {
    try {
      client.get(key)
    } catch {
      case ex: Exception =>
        val slot = JedisClusterCRC16.getSlot(key)
        val hash = getHash(key)
        logger.debug(s"Failed to get key ${hash} from Redis for slot ${slot}", ex)
        registry
          .counter(readExs.withTags("call", "get", "ex", ex.getClass.getSimpleName))
          .increment()
        null
    }
  }

  override def updateCache(
    key: Any,
    prev: CloudWatchCacheEntry,
    entry: CloudWatchCacheEntry,
    expiration: Long
  ): Unit = {
    try {
      cas(prev.toByteArray, entry, key.asInstanceOf[Array[Byte]], expiration)
    } catch {
      case ex: Exception =>
        // TODO - watch out, this could be really nasty if the cluster or node  is down
        val slot = JedisClusterCRC16.getSlot(key.asInstanceOf[Array[Byte]])
        val hash = getHash(key.asInstanceOf[Array[Byte]])
        logger.warn(s"Failed to set key ${hash} in Redis for slot ${slot}", ex)
        registry
          .counter(writeExs.withTags("call", "setGet", "ex", ex.getClass.getSimpleName))
          .increment()
    }
  }

  override def publish(scrapeTimestamp: Long): Future[NotUsed] = {
    if (!leaderStatus.hasLeadership) {
      logger.debug("Not the leader, skipping publishing.")
      return Future.successful(NotUsed)
    }

    val promise = Promise[NotUsed]()
    currentScan.set(promise.future)

    if (!running.compareAndSet(false, true)) {
      scrapeFailure.increment()
      logger.error(s"Failed to run the scrape at @${scrapeTimestamp} as another scrape is running.")
      return Future.failed(new RuntimeException("Another scrape is currently running."))
    }

    val start = registry.clock().monotonicTime()
    try {
      pubCounter.set(0)
      val scanners = Vector.newBuilder[Future[NotUsed]]
      logger.info(s"Starting Redis scrape and publish for ${scrapeTimestamp}")

      jedis.getClusterNodes.forEach((node, pool) => {
        // TODO / WARNING - This is brittle but I know of no other way to determine
        // if a node is a leader or not.
        try {
          val info = Using.resource(pool.getResource) { jedis =>
            jedis.sendCommand(Command.INFO, "Replication")
            jedis.getBulkReply()
          }
          if (info.contains("role:master")) {
            logger.info(s"Scanning Redis leader ${node}")
            val batchPromise = Promise[NotUsed]()
            scanners += batchPromise.future

            scanKeys(node)
              .onComplete {
                case Success(slotToKeys) =>
                  logger.debug(s"Finished slot to keys call with ${slotToKeys.size} slots")
                  val batches = ArrayBuffer[Future[NotUsed]]()
                  slotToKeys.values.foreach(keys => {
                    batches += getAndPublish(node, keys, scrapeTimestamp)
                  })

                  Future.sequence(batches).onComplete {
                    case Success(_) =>
                      batchPromise.complete(Try(NotUsed))
                    case Failure(ex) =>
                      // shouldn't happen.
                      logger.error("Failed on one or more batches", ex)
                      registry.counter(batchFailure.withTag("node", node)).increment()
                      batchPromise.failure(ex)
                  }
                case Failure(ex) =>
                  logger.error(s"Failed to scan for keys on node ${node}", ex)
                  // let the other leaders complete.
                  registry.counter(scanFailure.withTag("node", node)).increment()
                  batchPromise.complete(Try(NotUsed))
              }
          } else {
            logger.debug(s"Skipping Redis follower ${node}")
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to make info call for node ${node}", ex)
            registry
              .counter(readExs.withTags("call", "info", "ex", ex.getClass.getSimpleName))
              .increment()
        }
      })

      logger.info("Waiting on redis scrape to finish")
      Future.sequence(scanners.result()).onComplete {
        case Success(_) =>
          if (running.compareAndSet(true, false)) {
            val elapsed = registry.clock().monotonicTime() - start
            scrapeTime.record(elapsed, TimeUnit.NANOSECONDS)

            val duration = java.time.Duration.ofNanos(elapsed)
            logger.info(s"Finished a scrape run $duration with ${pubCounter.get()} pubs")
            promise.complete(Try(NotUsed))
          } else {
            logger.error(
              s"Failed to mark the scrape for ${start} as finished as it was already finished."
            )
            promise.failure(
              new IllegalStateException(
                s"Failed to mark the scrape for ${start} as " +
                  "finished as it was already finished."
              )
            )
          }
        case Failure(ex) =>
          running.set(false)
          logger.error(s"Failed to run scrape for ${start}", ex)
          scrapeFailure.increment()
          promise.failure(ex)
      }
    } catch {
      case ex: Exception =>
        running.set(false)
        logger.error(s"Unexpected exception scraping for ${start}", ex)
        scrapeFailure.increment()
        promise.failure(ex)
    }
    promise.future
  }

  private def scanKeys(node: String): Future[mutable.HashMap[Int, ArrayBuffer[Array[Byte]]]] = {
    val promise = Promise[mutable.HashMap[Int, ArrayBuffer[Array[Byte]]]]()
    executionContext.execute(new Runnable() {
      @Override def run(): Unit = {
        try {
          var sum = 0
          val slotToKeys = mutable.HashMap[Int, ArrayBuffer[Array[Byte]]]()
          Using.resource(jedis.getClusterNodes.get(node).getResource) { jedis =>
            try {
              var cursor: Array[Byte] = ScanParams.SCAN_POINTER_START_BINARY
              do {
                val scanResult = jedis.executeCommand(commandObjects.scan(cursor, scanParams))
                val keys = scanResult.getResult.asScala.toSeq
                logger.debug(s"Scanned keys ${keys.size} from node ${node}")
                if (keys.nonEmpty) {
                  keys.foreach(k => {
                    val slot = JedisClusterCRC16.getSlot(k)
                    val slotKeys = slotToKeys.getOrElseUpdate(slot, new ArrayBuffer[Array[Byte]]())
                    slotKeys += k
                  })
                  sum += keys.size
                }
                cursor = scanResult.getCursorAsBytes
              } while (!util.Arrays.equals(cursor, ScanParams.SCAN_POINTER_START_BINARY))
              true
            } catch {
              case ex: Exception =>
                logger.error(s"Failed to scan Redis host ${node}", ex)
                registry
                  .counter(readExs.withTags("call", "scan", "ex", ex.getClass.getSimpleName))
                  .increment()
                promise.failure(ex)
                false
            }
          } match {
            case true =>
              promise.complete(Try(slotToKeys))
            case _ => // no-op. Promise is set above.
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to get client for Redis host ${node}", ex)
            promise.failure(ex)
        }
      }
    })
    promise.future
  }

  def getAndPublish(
    node: String,
    keys: ArrayBuffer[Array[Byte]],
    scrapeTimestamp: Long,
    client: JedisCluster
  ): Future[NotUsed] = {
    val promise = Promise[NotUsed]()
    executionContext.execute(new Runnable() {
      override def run(): Unit = {
        registry.counter(keysRead.withTag("node", node)).increment()

        try {
          keys.sliding(maxBatch, maxBatch).foreach { batch =>
            try {
              registry.counter(batchCalls.withTag("node", node)).increment()
              val data = Using.resource(client.getClusterNodes.get(node).getResource) { jedis =>
                jedis.executeCommand(commandObjects.mget(batch.toSeq*))
              }

              var nonNull = 0
              batch
                .zip(data.asScala)
                .filter(_._2 != null)
                .foreach { t =>
                  sendToRouter(t._1, t._2, scrapeTimestamp)
                  nonNull += 1
                }
              registry.counter(entriesScraped.withTag("node", node)).increment(nonNull)
              keysPerBatch.record(batch.size)
              pubCounter.addAndGet(batch.size)
            } catch {
              case ex: Exception =>
                logger.error(s"Failed Redis MGET on node ${node}", ex)
                registry
                  .counter(readExs.withTags("call", "mget", "ex", ex.getClass.getSimpleName))
                  .increment()
            }
          }
          promise.complete(Try(NotUsed))

        } catch {
          case ex: Exception =>
            logger.error(s"Failed to get a Redis client for ${node}", ex)
            promise.failure(ex)
        }
      }
    })
    promise.future
  }

  override protected[cloudwatch] def delete(key: Any): Unit = {
    try {
      if (jedis.del(key.asInstanceOf[Array[Byte]]) == 0) {
        deleteFailures.increment()
      } else {
        deletes.increment()
      }
    } catch {
      case ex: Exception =>
        val slot = JedisClusterCRC16.getSlot(key.asInstanceOf[Array[Byte]])
        val hash = getHash(key.asInstanceOf[Array[Byte]])
        logger.debug(s"Failed to delete key ${hash} in Redis for slot ${slot}", ex)
        registry
          .counter(writeExs.withTags("call", "del", "ex", ex.getClass.getSimpleName))
          .increment()
    }
  }

  override protected[cloudwatch] def lastSuccessfulPoll(id: String): Long = {
    jedis.get((pollKey + id).getBytes("UTF-8")) match {
      case null  => 0
      case bytes => ByteBuffer.wrap(bytes).getLong()
    }
  }

  override protected[cloudwatch] def updateLastSuccessfulPoll(id: String, timestamp: Long): Unit = {
    val bytes = new Array[Byte](8)
    ByteBuffer.wrap(bytes).putLong(timestamp)
    jedis.set((pollKey + id).getBytes("UTF-8"), bytes) match {
      case null => logger.error(s"Failed to set last poll key ${pollKey + id}. Null response.")
      case "OK" => // no-op
      case unk  => logger.error(s"Failed to set last poll key ${pollKey + id}: ${unk}")
    }
  }

  /**
    * Psuedo compare and swap without having to try and install a Lua script in Redis. Simply sets the
    * value and compares the previous until it matches or we've tried 5 times. If a result is
    * returned that we didn't expect, we keep merging with the current value.
    *
    * @param prevBytes
    *     The previous value, may be null if we don't think anything existed.
    * @param current
    *     The current entry.
    * @param key
    *     The key to update.
    * @param expiration
    *     The expiration duration.
    */
  private[cloudwatch] def cas(
    prevBytes: Array[Byte],
    current: CloudWatchCacheEntry,
    key: Array[Byte],
    expiration: Long
  ): Unit = {
    performCas(jedis, prevBytes, current, key, expiration)
    performCas(valkeyJedis, prevBytes, current, key, expiration)
  }

  private def performCas(
    client: JedisCluster,
    prevBytes: Array[Byte],
    current: CloudWatchCacheEntry,
    key: Array[Byte],
    expiration: Long
  ): Unit = {
    var prev = prevBytes
    var newArray = current.toByteArray
    var fetched = client.setGet(key, newArray, SetParams.setParams().ex(expiration))
    var cntr = 0
    while (
      cntr < 5 &&
      casValidation(prev, fetched)
    ) {
      val merged = if (fetched == null) {
        current
      } else {
        merge(current, CloudWatchCacheEntry.parseFrom(fetched))
      }
      prev = newArray
      newArray = merged.toByteArray
      fetched = client.setGet(key, newArray, SetParams.setParams().ex(expiration))
      casCounter.increment()
      cntr += 1
    }

    if (casValidation(prev, fetched)) {
      val got = if (fetched == null) {
        "null"
      } else {
        CloudWatchCacheEntry.parseFrom(fetched).toString
      }
      val expected = if (prev == null) {
        "null"
      } else {
        CloudWatchCacheEntry.parseFrom(prev).toString
      }
      val original = if (prevBytes == null) {
        "null"
      } else {
        CloudWatchCacheEntry.parseFrom(prevBytes).toString
      }
      logger.warn(
        s"CAS Failure: Got $got but expected $expected. Current: $current.  Originally expected: $original"
      )
      casFailure.increment()
    }
  }

  private def casValidation(prev: Array[Byte], fetched: Array[Byte]): Boolean = {
    (prev, fetched) match {
      case (null, f) => f != null
      case (p, null) => p != null
      case (p, f)    => !p.sameElements(f)
    }
  }
}

object RedisClusterCloudWatchMetricsProcessor extends StrictLogging {

  private[cloudwatch] val keyArrays = ThreadLocal.withInitial[Array[Byte]](() => {
    val bytes = new Array[Byte](13)
    bytes(0) = 'c'
    bytes(1) = 'w'
    bytes(2) = 'f'
    bytes(3) = ':'
    bytes(4) = '{'
    bytes(12) = '}'
    bytes
  })

  private[cloudwatch] def getKey(hash: Long): Array[Byte] = {
    val bytes = keyArrays.get()
    ByteBuffer.wrap(bytes).putLong(4, hash)
    bytes
  }

  private[cloudwatch] def getHash(key: Array[Byte]): Long = {
    ByteBuffer.wrap(key).getLong(4)
  }

}
