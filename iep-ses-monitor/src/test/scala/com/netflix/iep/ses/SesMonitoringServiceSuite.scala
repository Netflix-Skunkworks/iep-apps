/*
 * Copyright 2014-2019 Netflix, Inc.
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
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.amazonaws.SdkClientException
import com.amazonaws.services.sqs.AbstractAmazonSQSAsync
import com.amazonaws.services.sqs.model.GetQueueUrlResult
import com.amazonaws.services.sqs.model.Message
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class SesMonitoringServiceSuite extends FunSuite with Matchers with BeforeAndAfterEach {

  private implicit val system: ActorSystem = ActorSystem()
  private implicit val mat: Materializer = ActorMaterializer()

  private object DummyAmazonSQSAsync extends AbstractAmazonSQSAsync

  private var sesMonitoringService: SesMonitoringService = _
  private var metricRegistry: Registry = _

  override def beforeEach(): Unit = {
    metricRegistry = new DefaultRegistry()
    sesMonitoringService = new SesMonitoringService(
      ConfigFactory.load(),
      metricRegistry,
      DummyAmazonSQSAsync,
      system,
      _ => Unit
    )
  }

  test("processed notifications should be marked for deletion") {

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    val processed = Await.result(
      Source
        .single(createNotificationMessage("{}"))
        .via(messageProcessingFlow)
        .runWith(Sink.head),
      2.seconds
    )

    processed.getClass shouldEqual classOf[MessageAction.Delete]
  }

  test(
    "notifications with no metadata should increment the ses.monitor.notifications metric " +
    "with dimension values of `unknown`."
  ) {

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "unknown")
        .withTag("sourceEmail", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage("{}"))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "bounce notifications delivered with no bounceRecipients increment ses.monitor.notifications"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "bounce notifications delivered with empty bounceRecipients increment ses.monitor.notifications"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\", \"bounceRecipients\": [ ]}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "bounce notifications delivered with one bounce recipient increment ses.monitor.notifications"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\", \"bouncedRecipients\": [ {\"emailAddress\": \"engineer@example.com\"} ]}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("bouncedRecipient", "engineer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.minutes
    )

    counter.count() shouldEqual 1
  }

  test(
    "bounce notifications delivered with multiple bounceRecipients increment " +
    "ses.monitor.notifications for each one"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\", \"bouncedRecipients\": [ {\"emailAddress\": \"engineer@example.com\"}, {\"emailAddress\": \"manager@example.com\"} ]}}"
        |}
      """.stripMargin

    val baseId = metricRegistry
      .createId("ses.monitor.notifications")
      .withTag("notificationType", "Bounce")
      .withTag("sourceEmail", "bouncer@example.com")
      .withTag("type", "Transient")
      .withTag("subType", "MailboxFull")

    val engineerCounter =
      metricRegistry.counter(baseId.withTag("bouncedRecipient", "engineer@example.com"))
    val managerCounter =
      metricRegistry.counter(baseId.withTag("bouncedRecipient", "manager@example.com"))

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    engineerCounter.count() shouldEqual 0
    managerCounter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    engineerCounter.count() shouldEqual 1
    managerCounter.count() shouldEqual 1
  }

  test(
    "bounce notifications delivered multiple times with the same metadata should increment " +
    "the ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Bounce")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "Transient")
        .withTag("subType", "MailboxFull")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

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

    counter.count() shouldEqual 5
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
        .withTag("sourceEmail", "unknown")
        .withTag("type", "unknown")
        .withTag("subType", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "complaint notifications delivered with all metadata increment the " +
    "ses.monitor.notifications metric"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Complaint\",\"mail\": {\"source\": \"bouncer@example.com\"},\"complaint\": {\"complaintFeedbackType\": \"not-spam\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Complaint")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "not-spam")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "complaint notifications delivered multiple times with the same metadata increment the " +
    "ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Complaint\",\"mail\": {\"source\": \"bouncer@example.com\"},\"complaint\": {\"complaintFeedbackType\": \"not-spam\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Complaint")
        .withTag("sourceEmail", "bouncer@example.com")
        .withTag("type", "not-spam")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

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

    counter.count() shouldEqual 3
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
        .withTag("sourceEmail", "unknown")
        .withTag("type", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "delivery notifications delivered with all metadata increment the " +
    "ses.monitor.notifications metric"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Delivery\",\"mail\": {\"source\": \"bouncer@example.com\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Delivery")
        .withTag("sourceEmail", "bouncer@example.com")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test(
    "delivery notifications delivered multiple times with the same metadata increment the " +
    "ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Delivery\",\"mail\": {\"source\": \"bouncer@example.com\"}}"
        |}
      """.stripMargin

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "Delivery")
        .withTag("sourceEmail", "bouncer@example.com")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

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

    counter.count() shouldEqual 6
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
        .withTag("sourceEmail", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    counter.count() shouldEqual 1
  }

  test("the raw notification message body should be logged") {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\"}}"
        |}
      """.stripMargin

    var loggerCalled = false

    val sesMonitoringService = new SesMonitoringService(
      ConfigFactory.load(),
      new NoopRegistry(),
      DummyAmazonSQSAsync,
      system,
      message => {
        message shouldEqual messageBody
        loggerCalled = true
      }
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    loggerCalled shouldEqual false

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    loggerCalled shouldEqual true
  }

  test("the raw notification message body should be logged, even if it can't be parsed as json") {

    val messageBody =
      """
        |{
        |  "Message": "{\"notificationType\": \"Bounce\",\"mail\": {\"source\": \"bouncer@example.com\"},\"bounce\": {\"bounceType\": \"Transient\",\"bounceSubType\": \"MailboxFull\"}"
        |}
      """.stripMargin

    var loggerCalled = false

    val sesMonitoringService = new SesMonitoringService(
      ConfigFactory.load(),
      new NoopRegistry(),
      DummyAmazonSQSAsync,
      system,
      message => {
        message shouldEqual messageBody
        loggerCalled = true
      }
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    loggerCalled shouldEqual false

    Await.result(
      Source
        .single(createNotificationMessage(messageBody))
        .via(messageProcessingFlow)
        .runWith(Sink.ignore),
      2.seconds
    )

    loggerCalled shouldEqual true
  }

  test("sqs queue uri is returned on success") {
    val result = new GetQueueUrlResult()
    val queueUri = "queueUri"
    result.setQueueUrl(queueUri)
    val counter = metricRegistry.counter(metricRegistry.createId("ses.monitor.getQueueUriFailure"))
    counter.count() shouldEqual 0

    val uriString = sesMonitoringService.getSqsQueueUri(result, JTDuration.ofMillis(0))

    counter.count() shouldEqual 0
    uriString shouldEqual queueUri
  }

  test(
    "multiple attempts are made to get the sqs queue uri if an unknown host exception occurs, " +
    "since there are occasional dns blips"
  ) {
    var attempts = 0
    val queueUri = "queueUri"

    def getUriSpy: GetQueueUrlResult = {
      attempts += 1

      if (attempts < 3) {
        throw new SdkClientException(new UnknownHostException("test"))
      }

      val result = new GetQueueUrlResult()
      result.setQueueUrl(queueUri)
      result
    }

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.getQueueUriFailure")
        .withTag("exception", "SdkClientException")
        .withTag("cause", "UnknownHostException")
    )

    counter.count() shouldEqual 0

    val uriString = sesMonitoringService.getSqsQueueUri(getUriSpy, JTDuration.ofMillis(10))

    counter.count() shouldEqual 2
    attempts shouldEqual 3
    uriString shouldEqual queueUri
  }

  test(
    "if an unexpected aws sdk exception occurs while getting the sqs queue uri, a failure is " +
    "recorded and it is rethrown"
  ) {
    var attempts = 0

    def getUriSpy: GetQueueUrlResult = {
      attempts += 1
      throw new SdkClientException(new NullPointerException("test"))
    }

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.getQueueUriFailure")
        .withTag("exception", "SdkClientException")
        .withTag("cause", "NullPointerException")
    )

    counter.count() shouldEqual 0

    intercept[SdkClientException] {
      sesMonitoringService.getSqsQueueUri(getUriSpy, JTDuration.ofMillis(10))
    }

    counter.count() shouldEqual 1
    attempts shouldEqual 1
  }

  test(
    "if an unexpected exception occurs while getting the sqs queue uri, a failure is " +
    "recorded and it is rethrown"
  ) {
    var attempts = 0

    def getUriSpy: GetQueueUrlResult = {
      attempts += 1
      throw new NullPointerException("test")
    }

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.getQueueUriFailure")
        .withTag("exception", "NullPointerException")
    )

    counter.count() shouldEqual 0

    intercept[NullPointerException] {
      sesMonitoringService.getSqsQueueUri(getUriSpy, JTDuration.ofMillis(10))
    }

    counter.count() shouldEqual 1
    attempts shouldEqual 1
  }

  private def createNotificationMessage(messageBody: String): Message = {
    new Message()
      .withMessageId("abc-123")
      .withReceiptHandle("abc-123-handle")
      .withBody(messageBody)
  }
}
