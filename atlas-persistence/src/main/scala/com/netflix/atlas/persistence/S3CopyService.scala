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
package com.netflix.atlas.persistence

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

class S3CopyService @Inject()(
  val config: Config,
  val registry: Registry,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  private val baseDir = config.getString("atlas.persistence.local-file.data-dir")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    logger.info("Starting service")
    killSwitch = Source
      .fromGraph(new FileWatchSource(baseDir)) //TODO wrap with RestartSource
      .filter(shouldCopy)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(new S3CopySink)(Keep.left)
      .run()
  }

  override def stopImpl(): Unit = {
    logger.info("Stopping service")
    if (killSwitch != null) killSwitch.shutdown()
  }

  // TODO handle .tmp after long idle?
  private def shouldCopy(f: File): Boolean = {
    !f.getName.endsWith(".tmp")
  }
}
