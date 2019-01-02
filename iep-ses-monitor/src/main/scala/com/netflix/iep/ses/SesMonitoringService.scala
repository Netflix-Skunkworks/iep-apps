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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.AbruptTerminationException
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.alpakka.sqs.SqsAckGroupedSettings
import akka.stream.alpakka.sqs.javadsl.SqsAckFlow
import akka.stream.alpakka.sqs.scaladsl.SqsSource
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message
import com.netflix.atlas.json.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.CardinalityLimiters
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.util.Failure
import scala.util.Success

class SesMonitoringService @Inject()(
  config: Config,
  registry: Registry,
  implicit val sqsAsync: AmazonSQSAsync,
  implicit val system: ActorSystem,
  sesNotificationLogger: NotificationLogger
) extends AbstractService
    with StrictLogging {

  private val receivedMessageCount = registry.counter("ses.monitor.receivedMessages")
  private val deletedMessageCount =
    registry.counter("ses.monitor.ackedMessages", "action", "delete")

  private val deserializationFailuresId = registry.createId("ses.monitor.deserializationFailures")

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

    val queueUrlResult = sqsAsync.getQueueUrl(notificationQueueName)
    val queueUrl = queueUrlResult.getQueueUrl

    logger.info(s"Connecting to SQS queue $notificationQueueName at $queueUrl")

    killSwitch = SqsSource(queueUrl)
      .via(createMessageProcessingFlow())
      .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.Defaults, sqsAsync))
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

  private[ses] def createMessageProcessingFlow(): Flow[Message, MessageAction, NotUsed] = {
    Flow[Message]
      .map { message =>
        receivedMessageCount.increment()
        message
      }
      .map(logNotification)
      .map(publishMetrics)
  }

  private[ses] def publishMetrics(message: Message): MessageAction = {
    try {
      // decoding to Map since we only record a few fields
      // ... may want to create a model object at some point
      val json = Json.decode[Map[String, Any]](message.getBody)
      val messageObject =
        Json.decode[Map[String, Any]](json.getOrElse("Message", "{}").asInstanceOf[String])
      incrementCounter(messageObject)
    } catch {
      case e: Exception =>
        registry.counter(deserializationFailuresId.withTag("exception", e.getClass.getSimpleName))
        logger.error(s"Error deserializing message: ${message.getBody}", e)
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

    val commonTags = Vector(
      new BasicTag(notificationTypeKey, notificationTypeValue),
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

  private[ses] def logNotification(message: Message): Message = {
    sesNotificationLogger.log(message.getBody)
    message
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }

}
