/*
 * Copyright 2014-2020 Netflix, Inc.
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

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.AbruptTerminationException
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.alpakka.sqs.SqsAckGroupedSettings
import akka.stream.alpakka.sqs.scaladsl.SqsAckFlow
import akka.stream.alpakka.sqs.scaladsl.SqsSource
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.netflix.atlas.json.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.CardinalityLimiters
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse
import software.amazon.awssdk.services.sqs.model.Message

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success

class SesMonitoringService @Inject()(
  config: Config,
  registry: Registry,
  implicit val sqsAsync: SqsAsyncClient,
  implicit val system: ActorSystem,
  sesNotificationLogger: NotificationLogger
) extends AbstractService
    with StrictLogging {

  private val receivedMessageCount = registry.counter("ses.monitor.receivedMessages")
  private val deletedMessageCount =
    registry.counter("ses.monitor.ackedMessages", "action", "delete")

  private val queueUrlFailureId = registry.createId("ses.monitor.getQueueUrlFailure")
  private val deserializationFailureId = registry.createId("ses.monitor.deserializationFailure")
  private val notificationLoggingFailureId =
    registry.createId("ses.monitor.notificationLoggingFailure")

  private val notificationsId = registry.createId("ses.monitor.notifications")
  private val sourceEmailLimiter = CardinalityLimiters.mostFrequent(20)
  private val bouncedRecipientLimiter = CardinalityLimiters.mostFrequent(20)

  private val streamFailures = registry.counter("ses.monitor.streamFailures")

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val mat = ActorMaterializer()

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {

    val notificationQueueName = config.getString("iep.ses.monitor.notification-queue-name")
    logger.debug(s"Getting queue URL for SQS queue $notificationQueueName")

    val getQueueUrlFutureTimeout =
      config.getDuration("iep.ses.monitor.sqs-get-queue-url-future-timeout")
    val queueUrl = getSqsQueueUrl(
      sqsAsync
        .getQueueUrl(GetQueueUrlRequest.builder().queueName(notificationQueueName).build())
        .get(getQueueUrlFutureTimeout.toMillis, TimeUnit.MILLISECONDS),
      config.getDuration("iep.ses.monitor.sqs-unknown-host-retry-delay")
    )

    logger.info(s"Connecting to SQS queue $notificationQueueName at $queueUrl")

    killSwitch = SqsSource(queueUrl)
      .via(createMessageProcessingFlow())
      .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.Defaults))
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) | Failure(_: AbruptTerminationException) =>
            // AbruptTerminationException will be triggered if the associated ActorSystem
            // is shutdown before the stream.
            logger.info(s"shutting down notification stream")
          case Failure(t) =>
            streamFailures.increment()
            logger.error(s"notification stream failed, attempting to restart", t)
            startImpl()
        }
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  @tailrec
  private[ses] final def getSqsQueueUrl(
    getQueueUrl: => GetQueueUrlResponse,
    retryDelay: Duration
  ): String = {
    import java.net.UnknownHostException

    import scala.util.Try

    Try(getQueueUrl) match {
      case Success(response) =>
        response.queueUrl

      case Failure(e) =>
        e.getCause match {
          case _: UnknownHostException =>
            logger.warn(
              "Unknown host getting SQS queue URI. This should clear on it's own. " +
              s"Retrying after delay of ${retryDelay}.",
              e
            )
            incrementQueueUrlFailure(e)
            Thread.sleep(retryDelay.toMillis)
            getSqsQueueUrl(getQueueUrl, retryDelay)
          case _ => {
            logger.error("Exception getting SQS queue URI.", e)
            incrementQueueUrlFailure(e)
            throw e
          }
        }
    }
  }

  private def incrementQueueUrlFailure(t: Throwable): Unit = {
    val id = Option(t.getCause).foldLeft {
      queueUrlFailureId.withTag("exception", t.getClass.getSimpleName)
    } {
      case (i, c) =>
        i.withTag("cause", c.getClass.getSimpleName)
    }

    registry.counter(id).increment()
  }

  private[ses] def createMessageProcessingFlow(): Flow[Message, MessageAction, NotUsed] = {
    Flow[Message]
      .map { message =>
        receivedMessageCount.increment()
        message
      }
      .map(logNotification)
      .map(processNotification)
  }

  private[ses] def processNotification(message: Message): MessageAction = {
    val (notificationAsMap, deserializationSucceeded) = try {
      // decoding to Map since we only record a few fields
      // ... may want to create a model object at some point
      val json = Json.decode[Map[String, Any]](message.body)
      val messageObject =
        Json.decode[Map[String, Any]](json.getOrElse("Message", "{}").asInstanceOf[String])
      incrementCounter(messageObject)
      (messageObject, true)
    } catch {
      case e: Exception =>
        registry
          .counter(deserializationFailureId.withTag("reason", e.getClass.getSimpleName))
          .increment()
        logger.error(s"Error deserializing JSON notification: ${message.body}", e)
        (Map.empty[String, Any], false)
    }

    try {
      if (notificationAsMap.nonEmpty) {
        val notificationJson = Json.encode(notificationAsMap)
        sesNotificationLogger.log(notificationJson)
      } else if (deserializationSucceeded) {
        registry
          .counter(notificationLoggingFailureId.withTag("reason", "EmptyJsonDocument"))
          .increment()
      }
    } catch {
      case e: Exception =>
        registry
          .counter(notificationLoggingFailureId.withTag("reason", e.getClass.getSimpleName))
          .increment()
        logger.error(s"Error logging notification JSON string: ${notificationAsMap}", e)
    }

    // Delete regardless of exception since it probably would throw again given the types of
    // exceptions that can happen during decode and extractTags
    deletedMessageCount.increment()
    MessageAction.delete(message)
  }

  private def getPathAsString(obj: Map[String, Any], path: String*): String = {
    getPath(obj, path: _*).fold("unknown")(_.asInstanceOf[String])
  }

  private def getPathAsList(
    obj: Map[String, Any],
    listElementKey: String,
    pathToList: String*
  ): List[String] = {
    getPath(obj, pathToList: _*).fold(List.empty[String]) {
      _.asInstanceOf[List[Map[String, Any]]].map(getPathAsString(_, listElementKey))
    }
  }

  @scala.annotation.tailrec
  private def getPath(obj: Map[String, Any], path: String*): Option[Any] = {
    // getOrElse defensively since notifications are an external input we don't control
    path.toList match {
      case ks if ks.isEmpty || obj.isEmpty =>
        None
      case k :: Nil =>
        obj.get(k)
      case k :: ks =>
        val subObj = obj.getOrElse(k, Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
        getPath(subObj, ks: _*)
    }
  }

  private[ses] def incrementCounter(notification: Map[String, Any]): Unit = {
    val notificationTypeKey = "notificationType"
    val notificationTypeValue = getPathAsString(notification, notificationTypeKey)

    val sourceEmail = getPathAsString(notification, "mail", "source")
    val sendingAccountId = getPathAsString(notification, "mail", "sendingAccountId")

    val commonTags = Vector(
      new BasicTag(notificationTypeKey, notificationTypeValue),
      new BasicTag("sendingAccountId", sendingAccountId),
      new BasicTag("sourceEmail", sourceEmailLimiter(sourceEmail))
    )

    notificationTypeValue match {
      case "Bounce" =>
        incrementBounce(notification, commonTags)
      case "Complaint" =>
        incrementComplaint(notification, commonTags)
      case _ =>
        registry.counter(notificationsId.withTags(commonTags: _*)).increment()
    }
  }

  private def incrementComplaint(
    notification: Map[String, Any],
    commonTags: Vector[BasicTag]
  ): Unit = {
    val tags = commonTags ++ Vector(
        new BasicTag("type", getPathAsString(notification, "complaint", "complaintFeedbackType"))
      )
    registry.counter(notificationsId.withTags(tags: _*)).increment()
  }

  private def incrementBounce(
    notification: Map[String, Any],
    commonTags: Vector[BasicTag]
  ): Unit = {
    val tags = commonTags ++ Vector(
        new BasicTag("type", getPathAsString(notification, "bounce", "bounceType")),
        new BasicTag("subType", getPathAsString(notification, "bounce", "bounceSubType"))
      )
    val idWithCommonTags = notificationsId.withTags(tags: _*)
    val bouncedRecipients =
      getPathAsList(notification, "emailAddress", "bounce", "bouncedRecipients")
    if (bouncedRecipients.isEmpty)
      registry.counter(idWithCommonTags).increment()
    else
      bouncedRecipients.foreach { br =>
        registry
          .counter(idWithCommonTags.withTag("bouncedRecipient", bouncedRecipientLimiter(br)))
          .increment()
      }
  }

  private def logNotification(message: Message): Message = {
    logger.debug(message.body)
    message
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }

}
