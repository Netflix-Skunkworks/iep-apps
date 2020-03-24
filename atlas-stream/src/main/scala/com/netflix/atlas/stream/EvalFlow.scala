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

import java.util.UUID

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging
import org.reactivestreams.Publisher

/**
  * This is the flow does the async evaluation of DataSource's:
  * 1. take DataSource's from client and delegates eval to EvalService
  * 2. consumes output from EvalService and push out as one single Source
  */
private[stream] class EvalFlow(
  evalService: EvalService,
  validator: DataSourceValidator
) extends GraphStage[FlowShape[String, Source[MessageEnvelope, NotUsed]]]
    with StrictLogging {

  private val in = Inlet[String]("EvalFlow.in")
  private val out = Outlet[Source[MessageEnvelope, NotUsed]]("EvalFlow.out")

  override val shape: FlowShape[String, Source[MessageEnvelope, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private val streamId = UUID.randomUUID().toString
      private var queue: StreamOps.SourceQueue[MessageEnvelope] = _
      private var pub: Publisher[MessageEnvelope] = _
      private var messageSourcePushed = false

      override def preStart(): Unit = {
        evalService.unregister(streamId)
        val (_queue, _pub) = evalService.register(streamId)
        queue = _queue
        pub = _pub
        // Need to pull at beginning because pull is triggered by onPush only
        pull(in)
      }

      // Source of MessageEnvelope should be pushed out only once. And if downstream pulls again,
      // that means the pushed Source has completed (could be caused by EvalService restart) and
      // the flow should be ended by calling completeStage.
      override def onPull(): Unit = {
        if (!messageSourcePushed) {
          push(out, sourceWithHeartbeat)
          messageSourcePushed = true
        } else {
          completeStage()
        }
      }

      private def sourceWithHeartbeat: Source[MessageEnvelope, NotUsed] = {
        import scala.concurrent.duration._
        val heartbeatSrc = Source
          .repeat(new MessageEnvelope("_", DiagnosticMessage.info("heartbeat")))
          .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
        Source
          .fromPublisher(pub)
          .merge(heartbeatSrc, eagerComplete = true) // Eager complete: heartbeat never completes
      }

      override def onPush(): Unit = {
        validator.validate(grab(in)) match {
          case Left(errors) =>
            queue.offer(new MessageEnvelope("_", DiagnosticMessage.error(Json.encode(errors))))
          case Right(dss) =>
            evalService.updateDataSources(streamId, dss)
        }
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        super.completeStage()
        evalService.unregister(streamId)
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.failStage(ex)
        evalService.unregister(streamId)
      }

      override def onDownstreamFinish(): Unit = {
        super.onDownstreamFinish()
        evalService.unregister(streamId)
      }

      setHandlers(in, out, this)
    }
  }
}

object EvalFlow {

  def createEvalFlow(
    evalService: EvalService,
    validator: DataSourceValidator
  ): Flow[String, MessageEnvelope, NotUsed] = {
    Flow[String].via(new EvalFlow(evalService, validator)).flatMapConcat(src => src)
  }
}
