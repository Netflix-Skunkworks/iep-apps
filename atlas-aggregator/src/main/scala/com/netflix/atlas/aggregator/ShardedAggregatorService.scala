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
package com.netflix.atlas.aggregator

import com.netflix.atlas.core.util.RefIntHashMap
import com.netflix.atlas.pekko.StreamOps
import com.netflix.iep.config.ConfigListener
import com.netflix.iep.config.DynamicConfigManager
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.impl.Hash64
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration.FiniteDuration
import scala.util.Using

class ShardedAggregatorService(
  configManager: DynamicConfigManager,
  registry: Registry,
  client: PekkoClient,
  localService: AtlasAggregatorService
) extends AbstractService
    with Aggregator
    with StrictLogging {

  import ShardedAggregatorService.*

  private val listener = ConfigListener.forConfig("atlas.aggregator.shards", updateConfig)

  private val queueManager =
    new QueueManager(registry, configManager.get().getConfig("atlas.aggregator.shards"), client)

  private val random = new java.util.Random()

  @volatile private var trafficRatio: Double = 0.0

  private def updateConfig(config: Config): Unit = {
    logger.info(s"config updated: $config")
    trafficRatio = config.getDouble("traffic-ratio")

    val count = config.getInt("count")
    val pattern = config.getString("uri-pattern")
    val uris = (0 until count).map { i =>
      pattern.formatted(i)
    }.toList
    queueManager.setUris(uris)
  }

  override def startImpl(): Unit = {
    configManager.addListener(listener)
  }

  override def stopImpl(): Unit = {
    configManager.removeListener(listener)
  }

  private def shouldUpdateLocally: Boolean = {
    trafficRatio <= 0.0 || trafficRatio < random.nextDouble()
  }

  override def add(id: Id, value: Double): Unit = {
    if (shouldUpdateLocally)
      localService.add(id, value)
    else
      update(id, ADD, value)
  }

  override def max(id: Id, value: Double): Unit = {
    if (shouldUpdateLocally)
      localService.max(id, value)
    else
      update(id, MAX, value)
  }

  private def update(id: Id, op: Int, value: Double): Unit = {
    val entry = UpdateEntry(id, op, value)
    queueManager.offer(entry)
  }
}

object ShardedAggregatorService {

  private val ADD = 0
  private val MAX = 10

  case class UpdateEntry(id: Id, op: Int, value: Double)

  case class UpdateQueue(uri: String, queue: StreamOps.BlockingSourceQueue[UpdateEntry])

  class QueueManager(registry: Registry, config: Config, client: PekkoClient)
      extends StrictLogging {

    private val queueSize = config.getInt("queue-size")
    private val batchSize = config.getInt("batch-size")

    @volatile private var queues: ArraySeq[UpdateQueue] = ArraySeq.empty

    def setUris(uris: List[String]): Unit = {
      val qs = queues
      val currentQueues = qs.map(q => q.uri -> q).toMap

      // Setup new queues
      val newQueues = uris.map { uri =>
        currentQueues.get(uri) match {
          case Some(q) => q
          case None    => UpdateQueue(uri, createQueue(uri))
        }
      }

      // Mark old queues as complete
      val oldQueues = currentQueues -- uris
      oldQueues.values.foreach { q =>
        logger.info(s"shutting down queue: ${q.uri}")
        q.queue.complete()
      }

      // Update queues reference
      queues = newQueues.to(ArraySeq)
    }

    private def createQueue(uriStr: String): StreamOps.BlockingSourceQueue[UpdateEntry] = {
      logger.info(s"creating queue: $uriStr (queue-size: $queueSize, batch-size: $batchSize)")
      implicit val system: ActorSystem = client.system
      val uri = Uri(uriStr)
      val id = uri.authority.host.address()
      val queue = new ArrayBlockingQueue[UpdateEntry](queueSize)
      StreamOps
        .wrapBlockingQueue(registry, id, queue)
        .groupedWithin(batchSize, FiniteDuration(2, TimeUnit.SECONDS))
        .map { batch =>
          new PekkoClient.RequestTuple(uri, id, batch, encode)
        }
        .via(client.createPublisherFlow)
        .toMat(Sink.ignore)(Keep.left)
        .run()
    }

    private def encode(batch: AnyRef): ByteString = {
      encodeEntries(batch.asInstanceOf[Seq[UpdateEntry]])
    }

    def offer(entry: UpdateEntry): Unit = {
      val qs = queues
      val i = Hash64.reduce(entry.id.hashCode(), qs.size)
      qs(i).queue.offer(entry)
    }
  }

  def encodeEntries(batch: Seq[UpdateEntry]): ByteString = {
    // Create string table
    val stringTable = new RefIntHashMap[String]()
    batch.foreach { entry =>
      val id = entry.id
      id.forEach { (k, v) =>
        stringTable.put(k, 0)
        stringTable.put(v, 0)
      }
    }
    val strings = new Array[String](stringTable.size)
    var i = 0
    stringTable.foreach { (k, _) =>
      strings(i) = k
      i += 1
    }
    java.util.Arrays.sort(strings.asInstanceOf[Array[AnyRef]])

    // Start generating output
    val baos = PekkoClient.getOrCreateStream
    Using.resource(PekkoClient.factory.createGenerator(baos)) { gen =>
      gen.writeStartArray()
      gen.writeNumber(strings.length)
      var i = 0
      while (i < strings.length) {
        gen.writeString(strings(i))
        stringTable.put(strings(i), i)
        i += 1
      }

      batch.foreach { entry =>
        val id = entry.id
        gen.writeNumber(id.size())
        id.forEach { (k, v) =>
          gen.writeNumber(stringTable.get(k, -1))
          gen.writeNumber(stringTable.get(v, -1))
        }
        gen.writeNumber(entry.op)
        gen.writeNumber(entry.value)
      }
      gen.writeEndArray()
    }

    ByteString.fromArrayUnsafe(baos.toByteArray)
  }
}
