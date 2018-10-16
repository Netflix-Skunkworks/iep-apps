/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.amazonaws.services.sqs.AbstractAmazonSQSAsync
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

  test("processed notifications should be marked for deletion") {

    val counter = metricRegistry.counter(
      metricRegistry
        .createId("ses.monitor.notifications")
        .withTag("notificationType", "unknown")
        .withTag("sourceEmail", "unknown")
    )

    val messageProcessingFlow = sesMonitoringService.createMessageProcessingFlow()

    counter.count() shouldEqual 0

    val processed = Await.result(
      Source
        .single(createNotificationMessage("{}"))
        .via(messageProcessingFlow)
        .runWith(Sink.head),
      2.seconds
    )

    processed.getClass shouldEqual classOf[MessageAction.Delete]
  }

  test("bounce notifications delivered with all metadata increment ses.monitor.notifications") {

    val messageBody =
      """
            |{
            |  "Message": {
            |    "notificationType": "Bounce",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    },
            |    "bounce": {
            |      "bounceType": "Transient",
            |      "bounceSubType": "MailboxFull"
            |    }
            |  }
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
    "bounce notifications delivered multiple times with the same metadata should increment " +
    "the ses.monitor.notifications metric equal to the number of times"
  ) {

    val messageBody =
      """
            |{
            |  "Message": {
            |    "notificationType": "Bounce",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    },
            |    "bounce": {
            |      "bounceType": "Transient",
            |      "bounceSubType": "MailboxFull"
            |    }
            |  }
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
            |  "Message": {
            |    "notificationType": "Bounce"
            |  }
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
            |  "Message": {
            |    "notificationType": "Complaint",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    },
            |    "complaint": {
            |      "complaintFeedbackType": "not-spam"
            |    }
            |  }
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
            |  "Message": {
            |    "notificationType": "Complaint",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    },
            |    "complaint": {
            |      "complaintFeedbackType": "not-spam"
            |    }
            |  }
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
            |  "Message": {
            |    "notificationType": "Complaint"
            |  }
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
            |  "Message": {
            |    "notificationType": "Delivery",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    }
            |  }
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
            |  "Message": {
            |    "notificationType": "Delivery",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    }
            |  }
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
            |  "Message": {
            |    "notificationType": "Delivery"
            |  }
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
            |  "Message": {
            |    "notificationType": "Bounce",
            |    "mail": {
            |      "source": "bouncer@example.com"
            |    },
            |    "bounce": {
            |      "bounceType": "Transient",
            |      "bounceSubType": "MailboxFull"
            |    }
            |  }
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

  private def createNotificationMessage(messageBody: String) = {
    new Message()
      .withMessageId("abc-123")
      .withReceiptHandle("abc-123-handle")
      .withBody(messageBody)
  }
}
