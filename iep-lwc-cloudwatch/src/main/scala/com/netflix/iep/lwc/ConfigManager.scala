/*
 * Copyright 2014-2022 Netflix, Inc.
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

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
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

      override def onPush(): Unit = {
        val msg = grab(in)
        if (msg.response.isUpdate) {
          val cluster = msg.cluster
          try {
            configs += cluster -> msg.response.clusterConfig
            logger.info(s"updated configuration for cluster $cluster")
          } catch {
            case e: Exception =>
              logger.warn(s"invalid config for cluster $cluster", e)
          }
        } else {
          configs -= msg.cluster
          logger.info(s"deleted configuration for cluster ${msg.cluster}")
        }
        push(out, configs.toMap)
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
