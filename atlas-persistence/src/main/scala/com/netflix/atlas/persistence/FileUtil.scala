/*
 * Copyright 2014-2026 Netflix, Inc.
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
import java.nio.file.Files

import com.typesafe.scalalogging.StrictLogging

import scala.jdk.StreamConverters.*
import scala.util.Using

object FileUtil extends StrictLogging {

  def delete(f: File): Unit = {
    try {
      Files.delete(f.toPath)
      logger.debug(s"deleted file $f")
    } catch {
      case e: Exception => logger.error(s"failed to delete path $f", e)
    }
  }

  def listFiles(f: File): List[File] = {
    try {
      Using.resource(Files.list(f.toPath)) { dir =>
        dir.toScala(List).map(_.toFile)
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to list files for: $f", e)
        Nil
    }
  }

  def isTmpFile(f: File): Boolean = {
    isTmpFile(f.getName)
  }

  def isTmpFile(s: String): Boolean = {
    s.endsWith(RollingFileWriter.TmpFileSuffix)
  }
}
