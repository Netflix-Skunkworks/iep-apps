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

import com.netflix.atlas.core.model.Datapoint
import com.typesafe.scalalogging.StrictLogging

trait RollingFileWriter extends StrictLogging {

  def initialize: Unit

  def write(dp: Datapoint): Unit = {
    if (shouldRollOver) {
      finishCurrentWriter
      newWriter
    }
    writeIt(dp)
  }

  def close(): Unit = finishCurrentWriter // Assuming rollOver closes current file

  protected[this] def writeIt(dp: Datapoint): Unit
  protected[this] def finishCurrentWriter: Unit
  protected[this] def newWriter: Unit
  protected[this] def shouldRollOver: Boolean
}