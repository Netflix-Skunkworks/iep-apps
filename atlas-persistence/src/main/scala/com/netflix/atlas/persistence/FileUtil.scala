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
