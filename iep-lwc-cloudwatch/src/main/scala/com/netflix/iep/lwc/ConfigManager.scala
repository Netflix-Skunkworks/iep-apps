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
package com.netflix.iep.lwc

import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import com.netflix.iep.lwc.ForwardingService.Message
import com.netflix.iep.lwc.fwd.cw.ClusterConfig
import com.typesafe.scalalogging.StrictLogging

class ConfigManager
    extends GraphStage[FlowShape[Message, Map[String, ClusterConfig]]]
    with StrictLogging {

  private val in = Inlet[Message]("ConfigManager.in")
  private val out = Outlet[Map[String, ClusterConfig]]("ConfigManager.out")

  override val shape: FlowShape[Message, Map[String, ClusterConfig]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private val configs = scala.collection.mutable.AnyRefMap.empty[String, ClusterConfig]
      private var changed = false

      override def onPush(): Unit = {
        val msg = grab(in)
        if (msg.isUpdate) {
          if (msg.response.isUpdate) {
            val cluster = msg.cluster
            try {
              configs += cluster -> msg.response.clusterConfig
              logger.info(s"updated configuration for cluster $cluster")
              changed = true
            } catch {
              case e: Exception =>
                logger.warn(s"invalid config for cluster $cluster", e)
            }
          } else {
            configs -= msg.cluster
            logger.info(s"deleted configuration for cluster ${msg.cluster}")
            changed = true
          }
        }

        // When performing a sync there will be many expression updates quickly, to avoid
        // a high cost down stream for processing the combined data sources we only do a push
        // when it is a done message or heartbeat.
        if ((msg.isDone || msg.isHeartbeat) && changed) {
          changed = false
          push(out, configs.toMap)
        } else {
          pull(in)
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

      setHandlers(in, out, this)
    }
  }
}
