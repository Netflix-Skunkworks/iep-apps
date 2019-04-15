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

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging

object Util extends StrictLogging {

  def createBackgroundThread(
    name: String,
    interval: Duration,
    callback: () => Unit
  ): Thread = {
    new Thread(
      () => {
        logger.info(s"$name started")

        while (true) {
          try {
            callback()
          } catch {
            case e: Exception =>
              logger.error(s"error in $name: ${e.getMessage}", e)
          }

          Thread.sleep(interval.toMillis)
        }
      },
      s"$name"
    )
  }

}
