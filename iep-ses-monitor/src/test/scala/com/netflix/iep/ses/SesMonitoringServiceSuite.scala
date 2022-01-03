/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.iep.ses

import java.net.UnknownHostException
import java.time.{Duration => JTDuration}

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse
import software.amazon.awssdk.services.sqs.model.Message

import scala.concurrent.Await
import scala.concurrent.duration._

class SesMonitoringServiceSuite extends FunSuite {

  private implicit val system: ActorSystem = ActorSystem()

  private object DummyAmazonSQSAsync extends SqsAsyncClient {
    override def serviceName(): String = getClass.getSimpleName

    override def close(): Unit = {}
  }

  private var sesMonitoringService: SesMonitoringService = _
  private var metricRegistry: Registry = _

  override def beforeEach(context: BeforeEach): Unit = {
    setup(new DefaultRegistry(), _ => ())
  }

  private def setup(testRegistry: Registry, notificationLoggerSpy: NotificationLogger): Unit = {
    metricRegistry = testRegistry
    sesMonitoringService = new SesMonitoringService(
      ConfigFactory.load(),
      metricRegistry,
      DummyAmazonSQSAsync,
      system,
      notificationLoggerSpy
    )
  }

  test("processed notifications should be marked for deletion") {

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    val processed = Await.result(
      Source
        .single(createNotificationMessage("{}"))
        .via(messageProcessingFlow)
        .runWith(Sink.head),
      10.seconds
    )

    assert(processed.getClass == classOf[MessageAction.Delete])
  }

  test(
    "notifications with no metadata should increment the ses.monitor.notifications metric " +
    "with dimension values of `unknown`."
  ) {

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "unknown")
        .withTag("sendingAccountId", "unknown")
        .withTag("sourceEmail", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage("{}"))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "bounce notifications delivered with no bounceRecipients increment ses.monitor.notifications"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "bounce notifications delivered with empty bounceRecipients increment ses.monitor.notifications"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\", \"bounceRecipients\": [ ]}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "bounce notifications delivered with one bounce recipient increment ses.monitor.notifications"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\", \"bouncedRecipients\": [ {\"emailAddress\": \"engineer@example.com\"} ]}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("bouncedRecipient", "engineer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.minutes
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "bounce notifications delivered with multiple bounceRecipients increment " +
    "ses.monitor.notifications for each one"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\", \"bouncedRecipients\": [ {\"emailAddress\": \"engineer@example.com\"}, {\"emailAddress\": \"manager@example.com\"} ]}}"
        |}
      """.stripMargin

    val baseId = metricRegistry
      .createId("ses.monitor.notifications")
      .withTag("notificationType", "Bounce")
      .withTag("sendingAccountId", "12345")
      .withTag("sourceEmail", "bouncer@example.com")
      .withTag("type", "Transient")
      .withTag("subType", "MailboxFull")

    val engineerCounter =
      metricRegistry.counter(baseId.withTag("bouncedRecipient", "engineer@example.com"))
    val managerCounter =
      metricRegistry.counter(baseId.withTag("bouncedRecipient", "manager@example.com"))

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(engineerCounter.count(), 0L)
    assertEquals(managerCounter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(engineerCounter.count(), 1L)
    assertEquals(managerCounter.count(), 1L)
  }

  test(
    "bounce notifications delivered multiple times with the same metadata should increment " +
    "the ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source(
        Vector(
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody)
        )
      ).via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 5L)
  }

  test(
    "bounce notifications delivered with missing metadata should increment the " +
    "ses.monitor.notifications metric with dimension values of `unknown` for the missing elements."
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\"}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sendingAccountId", "unknown")
        .withTag("sourceEmail", "unknown")
        .withTag("type", "unknown")
        .withTag("subType", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "complaint notifications delivered with all metadata increment the " +
    "ses.monitor.notifications metric"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Complaint\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"complaint\": {\"complaintFeedbackType\": \"not-spam\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Complaint")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "not-spam")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "complaint notifications delivered multiple times with the same metadata increment the " +
    "ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Complaint\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"complaint\": {\"complaintFeedbackType\": \"not-spam\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Complaint")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "not-spam")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source(
        Vector(
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody)
        )
      ).via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 3L)
  }

  test(
    "complaint notifications delivered with missing metadata increment the " +
    "ses.monitor.notifications metric with dimension values of `unknown` for the missing elements."
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Complaint\"}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Complaint")
        .withTag("sendingAccountId", "unknown")
        .withTag("sourceEmail", "unknown")
        .withTag("type", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "delivery notifications delivered with all metadata increment the " +
    "ses.monitor.notifications metric"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Delivery\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Delivery")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test(
    "delivery notifications delivered multiple times with the same metadata increment the " +
    "ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Delivery\",\"mail\": {\"source\": \"bouncer@example.com\", \"sendingAccountId\": \"12345\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Delivery")
        .withTag("sendingAccountId", "12345")
        .withTag("sourceEmail", "bouncer@example.com")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source(
        Vector(
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody),
          createNotificationMessage(messageBody)
        )
      ).via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 6L)
  }

  test(
    "delivery notifications delivered with missing metadata increment the " +
    "ses.monitor.notifications metric with dimension values of `unknown` for the missing elements."
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Delivery\"}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Delivery")
        .withTag("sendingAccountId", "unknown")
        .withTag("sourceEmail", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test("the raw notification message body should be logged") {

    val bounceNotification =
      """{"notificationType":"Bounce","mail":{"source":"bouncer@example.com","sendingAccountId":"12345"},"bounce":{"bounceType":"Transient","bounceSubType":"MailboxFull"}}"""

    val messageBody =
      s"""
        |{
        |  "Message":${Json.encode(bounceNotification)}
        |}
      """.stripMargin

    var loggerCalled = false
    var notificationBody = ""

    setup(new NoopRegistry(), message => {
      loggerCalled = true
      notificationBody = message
    })

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(loggerCalled, false)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(loggerCalled, true)
    assertEquals(notificationBody, bounceNotification)
  }

  test(
    "downstream consumers expect valid JSON so the raw notification message body should not be logged if it can't be parsed as json"
  ) {

    val messageBody =
      s"""
         |{
         |  "Message":{\"notificationType\":\"Bounce\",\"mail\":{\"source\":\"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\":{\"bounceType\":\"Transient\",\"bounceSubType\":\"MailboxFull\"}
         |}
      """.stripMargin

    var loggerCalled = false
    var msg = ""

    setup(new NoopRegistry(), message => {
      loggerCalled = true
      msg = s"logger should not have been called with invalid JSON: $message"
    })

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    assertEquals(loggerCalled, false)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(loggerCalled, false, msg)
  }

  test(
    "a raw notification message body that can't be parsed as json is handled by recording a metric"
  ) {

    val messageBody =
      s"""
         |{
         |  "Message":{\"notificationType\":\"Bounce\",\"mail\":{\"source\":\"bouncer@example.com\", \"sendingAccountId\": \"12345\"},\"bounce\":{\"bounceType\":\"Transient\",\"bounceSubType\":\"MailboxFull\"}
         |}
      """.stripMargin

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.deserializationFailure")
        .withTag("reason", "JsonEOFException")
    )
    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
    val notificationLoggingFailureCount = metricRegistry
      .counters()
      .filter(Functions.nameEquals("ses.monitor.notificationLoggingFailure"))
      .mapToLong(_.count)
      .reduce(0L, (left: Long, right: Long) => left + right)
    assertEquals(notificationLoggingFailureCount, 0L)

  }

  test(
    "an empty notification to log is handled by recording a metric, only if successfully deserialized"
  ) {

    val messageBody =
      s"""
         |{
         |  "Message":"{}"
         |}
      """.stripMargin

    setup(new DefaultRegistry(), _ => {
      throw new IllegalStateException("this exception should never cause the test to fail directly")
    })
    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notificationLoggingFailure")
        .withTag("reason", "EmptyJsonDocument")
    )
    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
    val deserializationFailureCount = metricRegistry
      .counters()
      .filter(Functions.nameEquals("ses.monitor.deserializationFailure"))
      .mapToLong(_.count)
      .reduce(0L, (left: Long, right: Long) => left + right)
    assertEquals(deserializationFailureCount, 0L)
  }

  test(
    "a notification logger that throws is handled by recording a metric"
  ) {

    val bounceNotification =
      """{"notificationType":"Bounce","mail":{"source":"bouncer@example.com","sendingAccountId":"12345"},"bounce":{"bounceType":"Transient","bounceSubType":"MailboxFull"}}"""

    val messageBody =
      s"""
         |{
         |  "Message":${Json.encode(bounceNotification)}
         |}
      """.stripMargin

    setup(new DefaultRegistry(), _ => {
      throw new IllegalStateException("this exception should never cause the test to fail directly")
    })
    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notificationLoggingFailure")
        .withTag("reason", "IllegalStateException")
    )
    assertEquals(counter.count(), 0L)

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    assertEquals(counter.count(), 1L)
  }

  test("sqs queue uri is returned on success") {
    val responseBuilder = GetQueueUrlResponse.builder()
    val queueUrl = "queueUrl"
    responseBuilder.queueUrl(queueUrl)
    val counter = metricRegistry.counter(metricRegistry.createId("ses.monitor.getQueueUrlFailure"))
    assertEquals(counter.count(), 0L)

    val response = responseBuilder.build()
    val uriString = sesMonitoringService.getSqsQueueUrl(response, JTDuration.ofMillis(0))

    assertEquals(counter.count(), 0L)
    assertEquals(uriString, queueUrl)
  }

  test(
    "multiple attempts are made to get the sqs queue uri if an unknown host exception occurs, " +
    "since there are occasional dns blips"
  ) {
    var attempts = 0
    val queueUrl = "queueUrl"

    def getUrlSpy: GetQueueUrlResponse = {
      attempts += 1

      if (attempts < 3) {
        throw SdkClientException.create("", new UnknownHostException("test"))
      }

      GetQueueUrlResponse.builder().queueUrl(queueUrl).build()
    }

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.getQueueUrlFailure")
        .withTag("exception", "SdkClientException")
        .withTag("cause", "UnknownHostException")
    )

    assertEquals(counter.count(), 0L)

    val uriString = sesMonitoringService.getSqsQueueUrl(getUrlSpy, JTDuration.ofMillis(10))

    assertEquals(counter.count(), 2L)
    assertEquals(attempts, 3)
    assertEquals(uriString, queueUrl)
  }

  test(
    "if an unexpected aws sdk exception occurs while getting the sqs queue uri, a failure is " +
    "recorded and it is rethrown"
  ) {
    var attempts = 0

    def getUrlSpy: GetQueueUrlResponse = {
      attempts += 1
      throw SdkClientException.create("", new NullPointerException("test"))
    }

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.getQueueUrlFailure")
        .withTag("exception", "SdkClientException")
        .withTag("cause", "NullPointerException")
    )

    assertEquals(counter.count(), 0L)

    intercept[SdkClientException] {
      sesMonitoringService.getSqsQueueUrl(getUrlSpy, JTDuration.ofMillis(10))
    }

    assertEquals(counter.count(), 1L)
    assertEquals(attempts, 1)
  }

  test(
    "if an unexpected exception occurs while getting the sqs queue uri, a failure is " +
    "recorded and it is rethrown"
  ) {
    var attempts = 0

    def getUrlSpy: GetQueueUrlResponse = {
      attempts += 1
      throw new NullPointerException("test")
    }

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.getQueueUrlFailure")
        .withTag("exception", "NullPointerException")
    )

    assertEquals(counter.count(), 0L)

    intercept[NullPointerException] {
      sesMonitoringService.getSqsQueueUrl(getUrlSpy, JTDuration.ofMillis(10))
    }

    assertEquals(counter.count(), 1L)
    assertEquals(attempts, 1)
  }

  private def createNotificationMessage(messageBody: String): Message = {
    Message
      .builder()
      .messageId("abc-123")
      .receiptHandle("abc-123-handle")
      .body(messageBody)
      .build()
  }
}
