package com.netflix.atlas.cloudwatch

import com.netflix.atlas.webapi.CloudWatchLogEvent
import munit.FunSuite
import org.apache.pekko.testkit.{ImplicitSender, TestKitBase}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OTelCloudWatchLogsProcessorSuite extends FunSuite with TestKitBase with Matchers {

  test("process sample mixed logs and capture unique patterns") {
    val processor = new TestableOTelCloudWatchLogsProcessor

    val owner = "282881007700"
    val logGroup = "/aws/lambda/LiveStackShadowTraffic"
    val logStream = "2026/04/11/[5]1dbca730f4644fa8a5ce4faab6c3e853"
    val filters = List("JSON_D")

    val baseTs = 1775890337826L
    var idx = 0

    def ev(message: String): CloudWatchLogEvent = {
      idx += 1
      CloudWatchLogEvent(
        id = s"event-$idx",
        timestamp = baseTs + idx,
        message = message,
        account = Some(owner),
        region = Some("us-east-1")
      )
    }

    val sampleMessages = List(
      "s6-rc: info: service nginx-monitor: starting",
      "2026/04/16 02:44:34 [notice] 105#105: using the \"epoll\" event method",
      "2026/04/16 02:44:34 [notice] 105#105: nginx/1.22.1",
      "2026/04/16 02:44:34 [notice] 105#105: OS: Linux 6.1.158-15.288.amzn2023.aarch64",
      "2026/04/16 02:44:34 [notice] 105#105: getrlimit(RLIMIT_NOFILE): 1024:1024",
      "2026/04/16 02:44:34 [notice] 105#105: start worker processes",
      "2026/04/16 02:44:34 [notice] 105#105: start worker process 117",
      "2026/04/16 02:44:34 [notice] 105#105: start worker process 118",
      "[NGINX-HEALTH] Checking nginx proxy readiness...",
      "s6-rc: info: service nginx-monitor successfully started",
      "[NGINX-MONITOR] Monitoring disabled (set NGINX_MONITOR_ENABLED=true to enable)",
      "2026/04/16 02:44:34 [warn] 118#118: *1 using uninitialized \"target\" variable while logging request, client: 127.0.0.1, server: _, request: \"GET / HTTP/1.1\", host: \"127.0.0.1:9124\"",
      "2026/04/16 02:44:34 [info] 118#118: *1 client 127.0.0.1 closed keepalive connection",
      "[NGINX-HEALTH] ✓ Nginx proxy is responding on port 9124 (attempt 1)",
      "[NGINX-HEALTH] ✓ Health check complete - nginx ready",
      "s6-rc: info: service nginx-health successfully started",
      "s6-rc: info: service claude-config: starting",
      "s6-rc: info: service python-app: starting",
      "s6-rc: info: service network-health: starting",
      "s6-rc: info: service python-app successfully started",
      "s6-rc: info: service network-health successfully started",
      "[PYTHON-APP] Starting Python application...",
      "[NGINX] Starting to tail nginx access logs...",
      "[PYTHON-APP] Running agent: agents.agent",
      "[PYTHON-APP] Working directory: /run/s6-rc:s6-rc-init:pBdfPM/servicedirs/python-app",
      "[NGINX] Log file found, tailing /tmp/nginx-access.log",
      "[CLAUDE-CONFIG] Processing Claude Code configuration template...",
      "[CLAUDE-CONFIG] Container mode: AGENT_CORE_RUNTIME",
      "[CLAUDE-CONFIG] Configured for AGENT_CORE_RUNTIME mode (nginx proxy)",
      "[NETWORK-HEALTH] Monitoring disabled (set NETWORK_HEALTH_ENABLED=true to enable)",
      """[NGINX] {"timestamp":"2026-04-16T02:44:34+00:00","session_id":"","container_id":"localhost","request_id":"bd8fa88e180303a1f51a99a80de64c3d","method":"GET","uri":"/","target":"","status":"200","upstream_response_time":"","request_time":"0.000","body_bytes_sent":"237"}""",
      "2026-04-16 02:44:34,988 [MainThread] [INFO] __main__: Starting rp-request-server on port 3143 ",
      "[CLAUDE-CONFIG] Claude Code configuration generated successfully",
      "  ANTHROPIC_BASE_URL: http://127.0.0.1:9124/proxy/modelgateway",
      "  CLAUDE_CODE_ENABLE_TELEMETRY: ",
      "  TRACE_TO_BRAINTRUST: true",
      "  PLUGIN_BRAINTRUST_ENABLED: true",
      "  Output: /home/app/.claude/settings.json",
      "s6-rc: info: service claude-config successfully started",
      "s6-rc: info: service claude-logs: starting",
      "s6-rc: info: service claude-logs successfully started",
      "s6-rc: info: service legacy-services: starting",
      "[CLAUDE-LOGS] Starting Claude Code debug log tailing...",
      "s6-rc: info: service legacy-services successfully started",
      "2026-04-16 02:44:37,525 - nflx_genai.agents.runner - INFO - Registered agent aimfautopragent_autopr (mode=invoke)",
      "2026-04-16 02:44:37,525 - nflx_genai.agents.internals.logging - INFO - nflx-otel-collector logging enabled",
      "2026-04-16 02:44:37,525 - nflx_genai.agents.runner - INFO - Importing agent module: agents.agent",
      "2026-04-16 02:44:37,526 - nflx_genai.agents.runner - INFO - Registered agent aimfautopragent_autopr (mode=invoke)",
      "2026-04-16 02:44:37,526 - nflx_genai.agents.runner - INFO - Auto-selected single registered agent 'aimfautopragent_autopr' from module 'agents.agent'",
      "2026-04-16 02:44:37,527 - nflx_genai.agents.runner - INFO - Starting server on 0.0.0.0:8080"
    )

    val events = sampleMessages.map(ev)

    // Invoke processor
    processor.process(
      owner = owner,
      logGroup = logGroup,
      logStream = logStream,
      subscriptionFilters = filters,
      events = events
    )

    val patterns = processor.newPatterns

    // Basic sanity: we should see some patterns, and count should be <= number of events
    patterns should not be empty
    patterns.size should be <= events.size

    // Helper to find a pattern containing some substring
    def patternContaining(sub: String): Option[(String, String, String)] =
      patterns.find { case (_, pattern, _) => pattern.contains(sub) }

    // Example assertions: we expect some representative patterns:

    // s6 service start/started pattern
    patternContaining("s6-rc: info: service nginx-monitor: starting") should not be empty

    // nginx notice pattern with TS and numeric normalization
    patternContaining("[notice]") should not be empty

    // nginx warn with IP
    patternContaining("[warn]") should not be empty

    // JSON nginx access log pattern
    patternContaining("[NGINX] {\"timestamp\":\"<TS>") should not be empty

    // Python logging style with nflx_genai
    patternContaining("nflx_genai.agents.runner - INFO - Starting server on <IP>:<NUM>") should not be empty
  }
}