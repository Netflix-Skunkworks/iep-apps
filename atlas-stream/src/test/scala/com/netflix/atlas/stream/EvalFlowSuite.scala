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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.spectator.api.NoopRegistry
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class EvalFlowSuite extends AnyFunSuite {
  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private val registry = new NoopRegistry()
  private val validateNoop: DataSource => Unit = _ => ()
  private val dataSourceStr =
    """[{"id":"abc", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,a,:eq"}]"""

  test("register and get message") {

    val evalService = new EvalService(registry, null, system) {
      override def updateDataSources(streamId: String, dataSources: DataSources): Unit = {
        val handler = getStreamInfo(streamId).handler
        handler.offer(new MessageEnvelope("mockId", DiagnosticMessage.info("mockMsg")))
        handler.complete()
      }
    }

    val evalFlow = EvalFlow.createEvalFlow(evalService, validateNoop)

    Source.single(dataSourceStr).via(evalFlow)
    val future = Source
      .single(dataSourceStr)
      .via(evalFlow)
      .filter(envelope => envelope.getId != "_") //filter out heartbeat
      .runWith(Sink.head)
    val messageEnvelope = Await.result(future, Duration.Inf)

    assert(messageEnvelope.getId === "mockId")
  }
}
