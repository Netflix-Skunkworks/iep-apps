/*
 * Copyright 2014-2023 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor.newCacheEntry
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.makeFirehoseMetric
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessorSuite.timestamp
import com.netflix.atlas.cloudwatch.RedisClusterCloudWatchMetricsProcessor.keyArrays
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.MockitoSugar.doAnswer
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.times
import org.mockito.MockitoSugar.verify
import org.mockito.MockitoSugar.when
import org.mockito.captor.ArgCaptor
import redis.clients.jedis.CommandObject
import redis.clients.jedis.Connection
import redis.clients.jedis.ConnectionPool
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.Protocol.Command
import redis.clients.jedis.args.RawableFactory.Raw
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult

import java.nio.ByteBuffer
import java.util
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class RedisClusterCloudWatchMetricsProcessorSuite extends FunSuite with TestKitBase {

  override implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  var client: JedisCluster = null
  var registry: Registry = null
  var leaderStatus: LeaderStatus = null
  var publishRouter: PublishRouter = null
  var routerCaptor = ArgCaptor[AtlasDatapoint]
  var clusterNodes: util.HashMap[String, ConnectionPool] = null

  val config = ConfigFactory.load()
  val tagger = new NetflixTagger(config.getConfig("atlas.cloudwatch.tagger"))
  val rules: CloudWatchRules = new CloudWatchRules(config)

  val category =
    MetricCategory("AWS/UTRedis", 60, 0, 2, None, List("node", "key"), List.empty, null)

  val firehoseMetric = makeFirehoseMetric(
    "AWS/UTRedis",
    "Redis",
    List.empty,
    Array(39.0, 1.0, 7.0, 19),
    "Count",
    timestamp
  )

  override def beforeEach(context: BeforeEach): Unit = {
    client = mock[JedisCluster]
    registry = new DefaultRegistry()
    leaderStatus = mock[LeaderStatus]
    publishRouter = mock[PublishRouter]
    routerCaptor = ArgCaptor[AtlasDatapoint]

    when(leaderStatus.hasLeadership).thenReturn(true)
  }

  test("updateCache new success") {
    mockRedis(firehoseMetric.xxHash, null)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.updateCache(firehoseMetric, category, timestamp + 60_000)
    assertCounters(updatesNew = 1)
  }

  test("updateCache existing success") {
    val cwDP = newCacheEntry(firehoseMetric, category)
    mockRedis(firehoseMetric.xxHash, cwDP.toByteArray)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.updateCache(firehoseMetric, category, timestamp + 60_000)
    assertCounters(updatesExisting = 1)
  }

  test("updateCache get exception") {
    mockRedis(firehoseMetric.xxHash, null, getEx = true)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.updateCache(firehoseMetric, category, timestamp + 60_000)
    assertCounters(readExs = Map("get" -> 1L))
  }

  test("updateCache setex exception") {
    mockRedis(firehoseMetric.xxHash, null, setEx = true)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.updateCache(firehoseMetric, category, timestamp + 60_000)
    assertCounters(writeExsSet = 1)
  }

  test("updateCache update exception via NPE") {
    mockRedis(firehoseMetric.xxHash, null, setEx = true)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.updateCache(null, category, timestamp + 60_000)
    assertCounters(updateExs = 1)
  }

  test("publish 1 leader success") {
    val redis = mockRedis(1, new Payload(1, 2))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(batchCallsL1 = 4, keysReadL1 = 4, entriesScrapedL1 = 4)
    assertPublished(
      List(
        ("l1", "1", 3),
        ("l1", "2", 3),
        ("l1", "3", 3),
        ("l1", "4", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 1 leader empty") {
    val redis = mockRedis(1, new Payload(1, 0))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters()
    assertPublished(List())
    assertResourceClose(redis)
  }

  test("publish 1 leader failed INFO call") {
    val redis = mockRedis(1)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(readExs = Map("info" -> 1L))
    assertPublished(List())
    assertResourceClose(redis)
  }

  test("publish 1 leader failed SCAN call") {
    val redis = mockRedis(1, new Payload(1, 2, withScanEx = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(scanFailureL1 = 1, readExs = Map("scan" -> 1L))
    assertPublished(List())
    assertResourceClose(redis)
  }

  test("publish 1 leader failed MGET call") {
    val redis = mockRedis(1, new Payload(1, 2, withBatchEx = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(
      keysReadL1 = 4,
      batchCallsL1 = 4,
      entriesScrapedL1 = 3,
      readExs = Map("mget" -> 1L)
    )
    assertPublished(
      List(
        ("l1", "1", 3), /*("l1", "2", 3),*/ ("l1", "3", 3),
        ("l1", "4", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 1 leader MGET expiration race") {
    val redis = mockRedis(1, new Payload(1, 2, withNulls = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(keysReadL1 = 4, batchCallsL1 = 4, entriesScrapedL1 = 3)
    assertPublished(
      List(
        ("l1", "1", 3), /*("l1", "2", 3),*/ ("l1", "3", 3),
        ("l1", "4", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 1 leader publish Exception") {
    when(publishRouter.publish(any[AtlasDatapoint])).thenAnswer((dp: AtlasDatapoint) =>
      if (dp.tags.get("key").get.equals("2")) throw new UTException("UT")
    )
    val redis = mockRedis(1, new Payload(1, 2, withNulls = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(keysReadL1 = 4, batchCallsL1 = 4, entriesScrapedL1 = 3)
    assertPublished(
      List(
        ("l1", "1", 3), /*("l1", "2", 3),*/ ("l1", "3", 3),
        ("l1", "4", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 2 leaders success") {
    val redis = mockRedis(2, new Payload(1, 2), new Payload(5, 1))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(
      batchCallsL1 = 4,
      batchCallsL2 = 2,
      keysReadL1 = 4,
      keysReadL2 = 2,
      entriesScrapedL1 = 4,
      entriesScrapedL2 = 2
    )
    assertPublished(
      List(
        ("l1", "1", 3),
        ("l1", "2", 3),
        ("l1", "3", 3),
        ("l1", "4", 3),
        ("l2", "5", 3),
        ("l2", "6", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 2 leaders 1 failed INFO call") {
    val redis = mockRedis(2, new Payload(1, 2), null)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(
      batchCallsL1 = 4,
      keysReadL1 = 4,
      entriesScrapedL1 = 4,
      readExs = Map("info" -> 1L)
    )
    assertPublished(
      List(
        ("l1", "1", 3),
        ("l1", "2", 3),
        ("l1", "3", 3),
        ("l1", "4", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 2 leaders 1 failed SCAN call") {
    val redis = mockRedis(2, new Payload(1, 2), new Payload(5, 1, withScanEx = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(
      batchCallsL1 = 4,
      keysReadL1 = 4,
      entriesScrapedL1 = 4,
      readExs = Map("scan" -> 1L),
      scanFailureL2 = 1
    )
    assertPublished(
      List(
        ("l1", "1", 3),
        ("l1", "2", 3),
        ("l1", "3", 3),
        ("l1", "4", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 2 leaders 1 failed MGET call") {
    val redis = mockRedis(2, new Payload(1, 2), new Payload(5, 1, withBatchEx = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(
      batchCallsL1 = 4,
      batchCallsL2 = 2,
      keysReadL1 = 4,
      keysReadL2 = 2,
      entriesScrapedL1 = 4,
      entriesScrapedL2 = 1,
      readExs = Map("mget" -> 1L)
    )
    assertPublished(
      List(
        ("l1", "1", 3),
        ("l1", "2", 3),
        ("l1", "3", 3),
        ("l1", "4", 3),
        /*("l2", "5", 3),*/ ("l2", "6", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish 2 leaders 1 MGET expiration race") {
    val redis = mockRedis(2, new Payload(1, 2), new Payload(5, 1, withNulls = true))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters(
      batchCallsL1 = 4,
      batchCallsL2 = 2,
      keysReadL1 = 4,
      keysReadL2 = 2,
      entriesScrapedL1 = 4,
      entriesScrapedL2 = 1
    )
    assertPublished(
      List(
        ("l1", "1", 3),
        ("l1", "2", 3),
        ("l1", "3", 3),
        ("l1", "4", 3),
        /*("l2", "5", 3),*/ ("l2", "6", 3)
      )
    )
    assertResourceClose(redis)
  }

  test("publish not primary") {
    when(leaderStatus.hasLeadership).thenReturn(false)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters()
  }

  test("publish pool exhaustion via NPE") {
    val redis = mockRedis(1, new Payload(1, 2))
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    clusterNodes.remove("l1")
    Await.result(proc.publish(timestamp), 5.seconds)
    assertCounters()
    assertResourceClose(redis)
  }

  test("delete success") {
    mockRedisDel(firehoseMetric.xxHash)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.delete(getKey(firehoseMetric.xxHash))
    assertCounters(deletes = 1)
  }

  test("delete failed") {
    mockRedisDel(firehoseMetric.xxHash, true)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.delete(getKey(firehoseMetric.xxHash))
    assertCounters(deleteFailures = 1)
  }

  test("delete exception") {
    mockRedisDel(firehoseMetric.xxHash, delEx = true)
    val proc = new RedisClusterCloudWatchMetricsProcessor(
      config,
      registry,
      tagger,
      client,
      leaderStatus,
      rules,
      publishRouter
    )(system)
    proc.delete(getKey(firehoseMetric.xxHash))
    assertCounters(writeExsDel = 1)
  }

  def assertPublished(expected: List[(String, String, Int)]): Unit = {
    verify(publishRouter, times(expected.map(_._3).sum)).publish(routerCaptor)
    val obtained = routerCaptor.values
    expected.foreach { exp =>
      val (node, key, count) = exp
      val filtered = obtained.filter(dp =>
        dp.tags.get("node").get.equals(node) && dp.tags.get("key").get.equals(key)
      )
      assertEquals(filtered.size, count)
    }
  }

  def assertCounters(
    updatesNew: Long = 0,
    updatesExisting: Long = 0,
    updateExs: Long = 0,
    scrapeFailures: Long = 0,
    batchFailuresL1: Long = 0,
    batchFailuresL2: Long = 0,
    scanFailureL1: Long = 0,
    scanFailureL2: Long = 0,
    readExs: Map[String, Long] = Map.empty,
    writeExsSet: Long = 0,
    writeExsDel: Long = 0,
    batchCallsL1: Long = 0,
    batchCallsL2: Long = 0,
    keysReadL1: Long = 0,
    keysReadL2: Long = 0,
    deletes: Long = 0,
    deleteFailures: Long = 0,
    entriesScrapedL1: Long = 0,
    entriesScrapedL2: Long = 0
  ): Unit = {
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.updates", "id", "new").count(),
      updatesNew
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.updates", "id", "existing").count(),
      updatesExisting
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.redis.updateExceptions", "ex", "NullPointerException")
        .count(),
      updateExs
    )
    assertEquals(registry.counter("atlas.cloudwatch.redis.scrapeFailures").count(), scrapeFailures)
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.batchFailures", "node", "l1").count(),
      batchFailuresL1
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.batchFailures", "node", "l2").count(),
      batchFailuresL2
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.scanFailure", "node", "l1").count(),
      scanFailureL1
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.scanFailure", "node", "l2").count(),
      scanFailureL2
    )
    List("get", "info", "scan", "mget").foreach { call =>
      assertEquals(
        registry
          .counter("atlas.cloudwatch.redis.readExceptions", "call", call, "ex", "UTException")
          .count(),
        readExs.getOrElse(call, 0L)
      )
    }
    assertEquals(
      registry
        .counter("atlas.cloudwatch.redis.writesExceptions", "call", "set", "ex", "UTException")
        .count(),
      writeExsSet
    )
    assertEquals(
      registry
        .counter("atlas.cloudwatch.redis.writesExceptions", "call", "del", "ex", "UTException")
        .count(),
      writeExsDel
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.batchCalls", "node", "l1").count(),
      batchCallsL1
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.batchCalls", "node", "l2").count(),
      batchCallsL2
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.keysRead", "node", "l1").count(),
      keysReadL1
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.keysRead", "node", "l2").count(),
      keysReadL2
    )
    assertEquals(registry.counter("atlas.cloudwatch.redis.deletes").count(), deletes)
    assertEquals(registry.counter("atlas.cloudwatch.redis.deleteFailures").count(), deleteFailures)
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.entriesScraped", "node", "l1").count(),
      entriesScrapedL1
    )
    assertEquals(
      registry.counter("atlas.cloudwatch.redis.entriesScraped", "node", "l2").count(),
      entriesScrapedL2
    )
  }

  def assertResourceClose(opensAndCloses: util.HashMap[String, Array[AtomicInteger]]): Unit = {
    opensAndCloses.entrySet().forEach { entry =>
      val counters = entry.getValue
      assertEquals(counters(0).get(), counters(1).get())
    }
  }

  def mockRedisDel(hash: Long, delFail: Boolean = false, delEx: Boolean = false): Unit = {
    val key = getKey(hash)
    if (delFail) {
      when(client.del(key)).thenReturn(0)
    } else if (delEx) {
      when(client.del(key)).thenThrow(new UTException("UT"))
    } else {
      when(client.del(key)).thenReturn(1)
    }
  }

  def mockRedis(
    hash: Long,
    existing: Array[Byte],
    getEx: Boolean = false,
    setEx: Boolean = false
  ): Unit = {
    val key = getKey(hash)
    if (getEx) {
      when(client.get(key)).thenThrow(new UTException("UT"))
    } else {
      when(client.get(key)).thenReturn(existing)
    }

    if (setEx) {
      when(client.setex(org.mockito.ArgumentMatchersSugar.eqTo(key), any[Long], any[Array[Byte]]))
        .thenThrow(new UTException("UT"))
    } else {
      when(client.setex(org.mockito.ArgumentMatchersSugar.eqTo(key), any[Long], any[Array[Byte]]))
        .thenReturn("OK")
    }
  }

  def mockRedis(leaders: Int, payloads: Payload*): util.HashMap[String, Array[AtomicInteger]] = {
    val resourceOpenClose = new util.HashMap[String, Array[AtomicInteger]]()
    clusterNodes = new util.HashMap[String, ConnectionPool]()
    for (i <- 1 to leaders) {
      // leader
      val pool = mock[ConnectionPool]
      val leaderResource = mock[Connection]
      resourceOpenClose.put(
        s"l${i}",
        // 0 == open, 1 == close
        Array[AtomicInteger](new AtomicInteger(), new AtomicInteger())
      )

      when(leaderResource.toString).thenReturn(s"l${i}")
      if (payloads == null || payloads.isEmpty) {
        when(leaderResource.getBulkReply()).thenThrow(new UTException("UT Info"))
      } else {
        if (payloads(i - 1) == null) {
          when(leaderResource.getBulkReply()).thenThrow(new UTException("UT Info"))
        } else {
          payloads(i - 1).setJedis(leaderResource)
          when(leaderResource.getBulkReply()).thenReturn("role:master")
        }
      }

      // capture open and close
      when(pool.getResource).thenAnswer {
        resourceOpenClose.get(s"l${i}")(0).incrementAndGet()
        leaderResource
      }
      doAnswer(resourceOpenClose.get(s"l${i}")(1).incrementAndGet())
        .when(leaderResource)
        .close()

      clusterNodes.put(s"l${i}", pool)

      // follower
      val followerPool = mock[ConnectionPool]
      val followerResource = mock[Connection]
      when(followerPool.getResource).thenReturn(followerResource)
      when(followerResource.toString).thenReturn(s"f${i}")
      clusterNodes.put(s"f${i}", followerPool)
    }
    when(client.getClusterNodes).thenReturn(clusterNodes)
    resourceOpenClose
  }

  def getKey(hash: Long): Array[Byte] = {
    val key = util.Arrays.copyOf(keyArrays.get(), 13)
    ByteBuffer.wrap(key).putLong(4, hash)
    return key
  }

  class Payload(
    start: Int,
    numScans: Int,
    withNulls: Boolean = false,
    withScanEx: Boolean = false,
    withBatchEx: Boolean = false
  ) {

    var scanned = 1
    var keyIdx = start
    val extractor = (key: Array[Byte]) => ByteBuffer.wrap(key).getLong(4)

    def setJedis(jedis: Connection): Unit = {
      when(jedis.executeCommand(any[CommandObject[AnyRef]])).thenAnswer((cmd: CommandObject[_]) => {
        // System.out.println(s"#### CMD: ${cmd.getArguments.getCommand}")
        cmd.getArguments.getCommand match {
          case Command.SCAN =>
            val scanResults = mock[ScanResult[Array[Byte]]]
            if (numScans <= 0 || scanned / 2 >= numScans) {
              if (withScanEx) {
                throw new UTException("UT Scan")
              }
              when(scanResults.getResult).thenReturn(Collections.emptyList())
              when(scanResults.getCursorAsBytes).thenReturn(ScanParams.SCAN_POINTER_START_BINARY)
            } else {
              RedisClusterCloudWatchMetricsProcessorSuite.this.synchronized {
                val scannedKeys = ArrayBuffer[Array[Byte]]()
                for (i <- keyIdx until keyIdx + 2) {
                  val b = util.Arrays.copyOf(keyArrays.get(), 12)
                  ByteBuffer.wrap(b).putLong(4, i)
                  scannedKeys += b
                }
                keyIdx += 2
                scanned += 2
                when(scanResults.getResult).thenReturn(scannedKeys.asJava)
                when(scanResults.getCursorAsBytes).thenReturn(new Array[Byte](keyIdx.toByte))
              }
            }
            scanResults
          case Command.MGET =>
            /*cmd.getArguments.forEach { arg =>
                System.out.println(s"   ARG: ${arg}")
              }*/
            val results = ArrayBuffer[Array[Byte]]()
            cmd.getArguments.forEach { arg =>
              if (arg.isInstanceOf[Raw]) {
                val key = extractor(arg.getRaw)
                if ((key == 2 || key == 5) && withBatchEx) {
                  throw new UTException("UT Batch EX")
                } else if ((key == 2 || key == 5) && withNulls) {
                  results += null
                } else {
                  results += makePayload(jedis.toString, key)
                }
              }
            }
            results.asJava
          case _ =>
            throw new IllegalStateException("Haven't mocked this yet.")
        }
      })
    }

    def makePayload(node: String, key: Long): Array[Byte] = {
      val category =
        MetricCategory("AWS/UTRedis", 60, 0, 2, None, List("node", "key"), List.empty, null)
      val cwDP = newCacheEntry(
        makeFirehoseMetric(
          "AWS/UTRedis",
          "Redis",
          List.empty,
          Array(39.0, 1.0, 7.0, 19),
          "Count",
          timestamp
        ),
        category
      )
      cwDP.toBuilder
        .addDimensions(
          CloudWatchDimension
            .newBuilder()
            .setName("node")
            .setValue(node)
            .build()
        )
        .addDimensions(
          CloudWatchDimension
            .newBuilder()
            .setName("key")
            .setValue(key.toString)
            .build()
        )
        .build()
        .toByteArray
    }
  }

  class UTException(msg: String) extends RuntimeException(msg)

}
