package com.netflix.iep.ses

trait NotificationLogger {
  def log(message: String): Unit
}
