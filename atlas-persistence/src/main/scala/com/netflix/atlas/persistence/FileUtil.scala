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

import java.nio.file.Files
import java.nio.file.Path

import com.typesafe.scalalogging.Logger

object FileUtil {

  def delete(path: Path, logger: Logger): Unit = {
    try {
      Files.delete(path)
      logger.debug(s"deleted path $path")
    } catch {
      case e: Exception => logger.error(s"failed to delete path $path", e)
    }
  }

  def move(source: Path, target: Path, logger: Logger): Unit = {
    try {
      Files.move(source, target)
    } catch {
      case e: Exception => logger.error(s"failed to move path from $source to $target", e)
    }
  }

}
