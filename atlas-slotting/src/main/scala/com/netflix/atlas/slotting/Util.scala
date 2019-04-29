/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.slotting

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.ScheduledFuture

import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.netflix.spectator.impl.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

object Util extends StrictLogging {

  def getLongOrDefault(config: Config, basePath: String): Long = {
    val env = NetflixEnvironment.accountEnv()
    val region = NetflixEnvironment.region()

    if (config.hasPath(s"$basePath.$env.$region"))
      config.getLong(s"$basePath.$env.$region")
    else
      config.getLong(s"$basePath.default")
  }

  def compress(s: String): ByteBuffer = {
    ByteBuffer.wrap(Gzip.compressString(s))
  }

  def decompress(buf: ByteBuffer): String = {
    Gzip.decompressString(toByteArray(buf))
  }

  def toByteArray(buf: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buf.remaining)
    buf.get(bytes, 0, bytes.length)
    buf.clear()
    bytes
  }

  def startScheduler(
    registry: Registry,
    name: String,
    interval: Duration,
    fn: () => Unit
  ): ScheduledFuture[_] = {
    val scheduler = new Scheduler(registry, name, 2)
    val options = new Scheduler.Options()
      .withFrequency(Scheduler.Policy.FIXED_RATE_SKIP_IF_LONG, interval)
    scheduler.schedule(options, () => fn())
  }

}
