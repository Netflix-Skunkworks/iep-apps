/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas.stream

import java.util.concurrent.ConcurrentHashMap

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.AbruptTerminationException
import akka.stream.ActorMaterializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.stream.EvalService.QueueHandler
import com.netflix.atlas.stream.EvalService.StreamInfo
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import org.reactivestreams.Publisher

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

class EvalService @Inject()(
  config: Config,
  val registry: Registry,
  val evaluator: Evaluator,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()
  private val registrations = new ConcurrentHashMap[String, StreamInfo]
  private val numDataSourceDistSum = registry.distributionSummary("evalService.numDataSource")
  private val queueSize = config.getInt("atlas.stream.eval-service.queue-size")

  override def startImpl(): Unit = {
    logger.debug("Starting service")

    // Cleanup for restarts
    cleanup()

    val dssSource: Source[DataSources, NotUsed] = Source
      .repeat(NotUsed)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      .map(_ => getCurrentDataSources)
      .mapMaterializedValue(_ => NotUsed)

    dssSource
      .via(evaluator.createStreamsFlow)
      .runForeach(distributeMessage)
      .onComplete {
        case Success(_) | Failure(_: AbruptTerminationException) =>
          // AbruptTerminationException will be triggered if the associated ActorSystem
          // is shutdown before the stream.
          logger.warn(s"Global eval stream completed")
        case Failure(t) =>
          logger.error(s"Global eval stream failed, attempting to restart", t)
          startImpl()
      }
  }

  override def stopImpl(): Unit = {
    cleanup()
  }

  /**
    * For now, not doing synchronization with `register`, considering below scenarios:
    * 1. registered but not completed here, then cleared from Map
    *    - EvalFlow gets an valid queue handle and setup successfully
    *    - later updateDataSources will fail because streamInfo is null, and flow will fail
    * 2. registered, completed here, and then cleared from Map
    *   - whole EvalFlow complete due to completion of queue handle
    */
  private def cleanup(): Unit = {
    registrations.values().forEach(streamInfo => streamInfo.handler.complete())
    registrations.clear()
  }

  // Distribute messages based the prefix of streamId in DataSource id
  private def distributeMessage(envelope: MessageEnvelope) = {
    try {
      // MessageEnvelope is DataSource id, which has been prefixed with "streamId" + "|"
      val index = envelope.getId.indexOf("|")
      if (index > 0) {
        val streamId = envelope.getId.substring(0, index)
        val info = getStreamInfo(streamId)
        if (info != null) {
          info.handler.offer(
            // Remove prefix
            new MessageEnvelope(envelope.getId.substring(index + 1), envelope.getMessage)
          )
        } else {
          logger.debug(s"discarding message without handler: $envelope")
        }
      } else {
        logger.debug(s"discarding message without streamId: $envelope")
      }
    } catch {
      case t: Exception => logger.debug(s"error distributing message: $envelope", t)
    }
  }

  def register(
    streamId: String
  ): (StreamOps.SourceQueue[MessageEnvelope], Publisher[MessageEnvelope]) = {
    val (queue, pub) = StreamOps
      .blockingQueue[MessageEnvelope](registry, "EvalService", queueSize)
      .toMat(Sink.asPublisher[MessageEnvelope](true))(Keep.both)
      .run()
    val handler = new QueueHandler[MessageEnvelope](streamId, queue)
    val prevValue = registrations.putIfAbsent(streamId, new StreamInfo(handler))
    if (prevValue == null) {
      logger.info(s"stream registered: $streamId")
    } else {
      throw new IllegalArgumentException(s"stream with id '$streamId' already registered")
    }

    (queue, pub)
  }

  def unregister(streamId: String): Unit = {
    try {
      val streamInfo = registrations.remove(streamId)
      if (streamInfo != null) {
        streamInfo.handler.complete()
      }
    } catch {
      case t: Exception => logger.error(s"Error unregistering stream $streamId", t)
    }
  }

  def updateDataSources(streamId: String, dataSources: DataSources): Unit = {
    val streamInfo = getStreamInfo(streamId)
    if (streamInfo == null) {
      throw new IllegalStateException(s"stream has not been registered: $streamId")
    }

    streamInfo.dataSources = Some(
      new DataSources(
        dataSources.getSources.asScala
          .map(
            ds =>
              // Prefix DataSource id with streamId+"|", for mapping MessageEnvelope to stream later
              new DataSource(s"$streamId|${ds.getId}", ds.getStep, ds.getUri)
          )
          .asJava
      )
    )
  }

  protected def getStreamInfo(streamId: String): StreamInfo = {
    registrations.get(streamId)
  }

  private def getCurrentDataSources: DataSources = {
    val dsSet = registrations.values.asScala
      .flatMap(_.dataSources)
      .flatMap(_.getSources.asScala)
      .toSet
      .asJava
    numDataSourceDistSum.record(dsSet.size())
    new DataSources(dsSet)
  }

}

object EvalService {

  class StreamInfo(
    val handler: QueueHandler[MessageEnvelope],
    var dataSources: Option[DataSources] = None
  )

  class QueueHandler[T](id: String, queue: StreamOps.SourceQueue[T]) extends StrictLogging {

    def offer(msg: T): Boolean = {
      queue.offer(msg)
    }

    def complete(): Unit = {
      logger.debug(s"queue complete for $id")
      queue.complete()
    }

    override def toString: String = s"QueueHandler($id)"
  }

}
