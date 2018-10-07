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
import org.scalatest.Matchers
import org.scalatest.WordSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class SesMonitoringServiceSuite extends WordSpec with Matchers with BeforeAndAfterEach {

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

  "An SES notification" when {
    "delivered with no metadata" should {
      "increment the ses.monitor.notifications metric with dimension values of `unknown`." in {

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
    }

    "processed" should {
      "be marked for deletion" in {

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
    }
  }

  "An SES Bounce notification" when {
    "delivered with all metadata" should {
      "increment the ses.monitor.notifications metric" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Bounce",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  },
            |  "bounce": {
            |    "bounceType": "Transient",
            |    "bounceSubType": "MailboxFull"
            |  }
            |}
          """.stripMargin

        val counter = metricRegistry.counter(
          metricRegistry
            .createId("ses.monitor.notifications")
            .withTag("notificationType", "Bounce")
            .withTag("sourceEmail", "bouncer@saasmail.netflix.com")
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
    }

    "delivered multiple times with the same metadata" should {
      "increment the ses.monitor.notifications metric equal to the number of times" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Bounce",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  },
            |  "bounce": {
            |    "bounceType": "Transient",
            |    "bounceSubType": "MailboxFull"
            |  }
            |}
          """.stripMargin

        val counter = metricRegistry.counter(
          metricRegistry
            .createId("ses.monitor.notifications")
            .withTag("notificationType", "Bounce")
            .withTag("sourceEmail", "bouncer@saasmail.netflix.com")
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
    }

    "delivered with missing metadata" should {
      "increment the ses.monitor.notifications metric with dimension values of `unknown` " +
      "for the missing elements." in {

        val messageBody =
          """
            |{
            |  "notificationType": "Bounce"
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
    }
  }

  "An SES Complaint notification" when {
    "delivered with all metadata" should {
      "increment the ses.monitor.notifications metric" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Complaint",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  },
            |  "complaint": {
            |    "complaintFeedbackType": "not-spam"
            |  }
            |}
          """.stripMargin

        val counter = metricRegistry.counter(
          metricRegistry
            .createId("ses.monitor.notifications")
            .withTag("notificationType", "Complaint")
            .withTag("sourceEmail", "bouncer@saasmail.netflix.com")
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
    }

    "delivered multiple times with the same metadata" should {
      "increment the ses.monitor.notifications metric equal to the number of times" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Complaint",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  },
            |  "complaint": {
            |    "complaintFeedbackType": "not-spam"
            |  }
            |}
          """.stripMargin

        val counter = metricRegistry.counter(
          metricRegistry
            .createId("ses.monitor.notifications")
            .withTag("notificationType", "Complaint")
            .withTag("sourceEmail", "bouncer@saasmail.netflix.com")
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
    }

    "delivered with missing metadata" should {
      "increment the ses.monitor.notifications metric with dimension values of `unknown` " +
      "for the missing elements." in {

        val messageBody =
          """
            |{
            |  "notificationType": "Complaint"
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
    }
  }

  "An SES Delivery notification" when {
    "delivered with all metadata" should {
      "increment the ses.monitor.notifications metric" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Delivery",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  }
            |}
          """.stripMargin

        val counter = metricRegistry.counter(
          metricRegistry
            .createId("ses.monitor.notifications")
            .withTag("notificationType", "Delivery")
            .withTag("sourceEmail", "bouncer@saasmail.netflix.com")
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
    }

    "delivered multiple times with the same metadata" should {
      "increment the ses.monitor.notifications metric equal to the number of times" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Delivery",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  }
            |}
          """.stripMargin

        val counter = metricRegistry.counter(
          metricRegistry
            .createId("ses.monitor.notifications")
            .withTag("notificationType", "Delivery")
            .withTag("sourceEmail", "bouncer@saasmail.netflix.com")
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
    }

    "delivered with missing metadata" should {
      "increment the ses.monitor.notifications metric with dimension values of `unknown` " +
      "for the missing elements." in {

        val messageBody =
          """
            |{
            |  "notificationType": "Delivery"
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
    }
  }

  "An SES notification" when {
    "delivered" should {
      "call the notification logger with the raw notification message body" in {

        val messageBody =
          """
            |{
            |  "notificationType": "Bounce",
            |  "mail": {
            |    "source": "bouncer@saasmail.netflix.com"
            |  },
            |  "bounce": {
            |    "bounceType": "Transient",
            |    "bounceSubType": "MailboxFull"
            |  }
            |}
          """.stripMargin

        var loggerCalled = false
        val loggerSpy = (message: String) => {
          message shouldEqual messageBody
          loggerCalled = true
        }

        val sesMonitoringService = new SesMonitoringService(
          ConfigFactory.load(),
          new NoopRegistry(),
          DummyAmazonSQSAsync,
          system,
          loggerSpy
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
    }
  }

  private def createNotificationMessage(messageBody: String) = {
    new Message()
      .withMessageId("abc-123")
      .withReceiptHandle("abc-123-handle")
      .withBody(messageBody)
  }
}
