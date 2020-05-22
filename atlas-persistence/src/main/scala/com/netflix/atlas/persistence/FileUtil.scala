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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import com.typesafe.scalalogging.Logger

object FileUtil {

  def delete(f: File, logger: Logger): Unit = {
    try {
      Files.delete(f.toPath)
      logger.debug(s"deleted file $f")
    } catch {
      case e: Exception => logger.error(s"failed to delete path $f", e)
    }
  }

  def listFiles(f: File, logger: Logger): List[File] = {
    try {
      f.listFiles().toList
    } catch {
      case e: Exception =>
        logger.error(s"failed to list files for: $f", e)
        Nil
    }
  }

  def isTmpFile(f: File): Boolean = {
    f.getName.endsWith(RollingFileWriter.TmpFileSuffix)
  }

  // Mark a file as complete by removing tmp suffix, so it's ready for s3 copy
  def markWriteComplete(f: File, logger: Logger): Unit = {
    try {
      val filePath = f.getCanonicalPath
      Files.move(
        Paths.get(filePath),
        Paths.get(filePath.substring(0, filePath.length - RollingFileWriter.TmpFileSuffix.length))
      )
    } catch {
      case e: Exception =>
        logger.error(s"Failed to mark file as complete by removing tmp suffix: $f", e)
    }
  }
}
