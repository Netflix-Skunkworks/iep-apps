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
package com.netflix.atlas.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class EvalFlowSuite extends FunSuite {

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  private val config = ConfigFactory.load
  private val registry = new NoopRegistry()
  private val validateNoop: DataSource => Unit = _ => ()

  private val dataSourceStr =
    """[{"id":"abc", "step": 10, "uri":"http://local-dev/api/v1/graph?q=name,a,:eq"}]"""

  test("register and get message") {

    val evalService = new EvalService(config, registry, null, system) {
      override def updateDataSources(streamId: String, dataSources: DataSources): Unit = {
        val handler = getStreamInfo(streamId).handler
        handler.offer(new MessageEnvelope("mockId", DiagnosticMessage.info("mockMsg")))
        handler.complete()
      }
    }

    val evalFlow = EvalFlow.createEvalFlow(evalService, DataSourceValidator(10, validateNoop))

    Source.single(dataSourceStr).via(evalFlow)
    val future = Source
      .single(dataSourceStr)
      .via(evalFlow)
      .filter(envelope => envelope.id() != "_") // filter out heartbeat
      .runWith(Sink.head)
    val messageEnvelope = Await.result(future, Duration.Inf)

    assertEquals(messageEnvelope.id(), "mockId")
  }
}
