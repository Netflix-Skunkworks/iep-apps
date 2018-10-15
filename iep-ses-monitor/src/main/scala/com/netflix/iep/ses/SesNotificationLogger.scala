package com.netflix.iep.ses
import org.slf4j.LoggerFactory

object SesNotificationLogger extends NotificationLogger {
  private val logger = LoggerFactory.getLogger("com.netflix.iep.ses.SesNotificationLogger")

  override def log(message: String): Unit = {
    logger.info(message)
  }
}
