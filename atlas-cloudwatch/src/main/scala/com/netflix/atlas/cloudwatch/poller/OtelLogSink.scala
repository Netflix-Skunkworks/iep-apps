package com.netflix.atlas.cloudwatch

trait OtelLogSink {
  def send(log: OtelLog): Unit
}
