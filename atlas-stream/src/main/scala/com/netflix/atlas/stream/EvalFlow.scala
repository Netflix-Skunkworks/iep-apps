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
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging
import org.reactivestreams.Publisher

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * This is the flow does the async evaluation of DataSource's:
  * 1. take DataSource's from client and delegates eval to EvalService
  * 2. consumes output from EvalService and push out as one single Source
  */
private[stream] class EvalFlow(
  evalService: EvalService,
  evaluator: Evaluator
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

      override def preStart(): Unit = {
        evalService.unregister(streamId)
        val (_queue, _pub) = evalService.register(streamId)
        queue = _queue
        pub = _pub
        // Need to pull at beginning because pull is triggered by onPush only
        pull(in)
      }
      override def onPull(): Unit = {
        if (isAvailable(out)) {
          push(out, sourceWithHeartbeat)
        }
      }

      private def sourceWithHeartbeat: Source[MessageEnvelope, NotUsed] = {
        import scala.concurrent.duration._
        val heartbeatSrc = Source
          .repeat(new MessageEnvelope("_", DiagnosticMessage.info("heartbeat")))
          .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
        Source
          .fromPublisher(pub)
          .merge(heartbeatSrc, true) // Eager complete: heartbeat never completes
      }

      override def onPush(): Unit = {
        val parser = DataSourcesParser(grab(in), evaluator.validate)

        if (parser.isValid) {
          evalService.updateDataSources(streamId, parser.dataSources)
        } else {
          // All or None, ignore all if any validation issue
          queue.offer(
            new MessageEnvelope(
              "_",
              DiagnosticMessage.error(
                s"Invalid input: ${parser.errors.mkString("{", ", ", "}")}"
              )
            )
          )
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

  // Parse and validation
  private case class DataSourcesParser(input: String, validateFunc: DataSource => Unit) {

    private val errorCollector = mutable.Map[String, ListBuffer[String]]()
    private var dataSourceList: List[DataSource] = List.empty[DataSource]
    var dataSources: DataSources = null

    // Class statements are executed in order, this should be after init of fields
    parseAndValidate()

    def isValid: Boolean = errorCollector.isEmpty

    // Validation errors as a map, key is the DataSource id, id "_" means it's a general error
    def errors: Map[String, String] = {
      errorCollector.map {
        case (k, v) => (k, v.mkString("[", ", ", "]"))
      }.toMap
    }

    private def parseAndValidate(): Unit = {
      // Parse json
      try {
        dataSourceList = Json.decode[List[DataSource]](input)
      } catch {
        case e: Exception => {
          logger.warn(s"failed to parse input: ${e.getMessage}")
          addError("_", e.getMessage)
        }
      }

      // Validate each DataSource
      val visitedIds = mutable.Set[String]()
      dataSourceList.foreach(ds => {
        val id = ds.getId

        // Validate id
        if (id == null || id.isEmpty) {
          addError("_", "id cannot be empty")
        } else {
          if (visitedIds.contains(id)) {
            addError(id, "id cannot be duplicated")
          } else {
            visitedIds.add(id)
          }
        }

        // Validate uri
        Try(validateFunc(ds)) match {
          case Success(_) =>
          case Failure(e) => addError(id, s"invalid uri: ${e.getMessage}")
        }
      })

      if (isValid) {
        dataSources = new DataSources(dataSourceList.toSet.asJava)
      }
    }

    private def addError(dsId: String, value: String): Unit = {
      errorCollector.getOrElseUpdate(dsId, new ListBuffer[String]()) += value
    }
  }
}

object EvalFlow {

  def createEvalFlow(
    evalService: EvalService,
    evaluator: Evaluator
  ): Flow[String, MessageEnvelope, NotUsed] = {
    Flow[String].via(new EvalFlow(evalService, evaluator)).flatMapConcat(src => src)
  }
}
