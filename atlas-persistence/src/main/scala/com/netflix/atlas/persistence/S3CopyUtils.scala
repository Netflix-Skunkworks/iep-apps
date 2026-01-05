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
import java.security.MessageDigest

object S3CopyUtils {

  def isInactive(f: File, maxInactiveMs: Long, now: Long = System.currentTimeMillis): Boolean = {
    val lastModified = f.lastModified()
    if (lastModified == 0) false
    else now > lastModified + maxInactiveMs
  }

  def shouldProcess(
    file: File,
    activeFiles: Set[String],
    maxInactiveMs: Long,
    isTmpFile: String => Boolean = FileUtil.isTmpFile
  ): Boolean = {
    if (activeFiles.contains(file.getName)) false
    else if (isTmpFile(file.getName)) {
      if (isInactive(file, maxInactiveMs)) true else false
    } else true
  }

  def buildS3Key(
    fileName: String,
    prefix: String,
    hourLen: Int = HourlyRollingWriter.HourStringLen
  ): String = {
    val hour = fileName.substring(0, hourLen)
    val s3FileName = fileName.substring(hourLen + 1)
    val hourPath = hash(s"$prefix/$hour")
    val startMinute = S3CopySink.extractMinuteRange(fileName)
    s"$hourPath/$startMinute/$s3FileName"
  }

  def hash(path: String): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(path.getBytes("UTF-8"))
    val digest = md.digest()
    val hexBytes = digest.take(2).map("%02x".format(_)).mkString
    val randomPrefix = hexBytes.take(3)
    s"$randomPrefix/$path"
  }
}
