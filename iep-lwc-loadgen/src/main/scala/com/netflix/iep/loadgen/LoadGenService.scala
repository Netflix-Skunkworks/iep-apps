/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.iep.loadgen

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.AbruptTerminationException
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.histogram.PercentileDistributionSummary
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.PercentileTimer
import com.netflix.spectator.api.patterns.CardinalityLimiters
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class LoadGenService(
  config: Config,
  registry: Registry,
  evaluator: Evaluator,
  implicit val system: ActorSystem
) extends AbstractService
    with StrictLogging {

  import LoadGenService.*

  private val streamFailures = registry.counter("loadgen.streamFailures")

  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val limiter = CardinalityLimiters.mostFrequent(20)

  private var killSwitch: KillSwitch = _

  override def startImpl(): Unit = {
    killSwitch = Source(dataSources)
      .flatMapMerge(Int.MaxValue, evalSource)
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) | Failure(_: AbruptTerminationException) =>
            // AbruptTerminationException will be triggered if the associated ActorSystem
            // is shutdown before the stream.
            logger.info(s"shutting down forwarding stream")
          case Failure(t) =>
            streamFailures.increment()
            logger.error(s"forwarding stream failed, attempting to restart", t)
            startImpl()
        }
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(updateStats))(Keep.left)
      .run()
  }

  override def stopImpl(): Unit = {
    if (killSwitch != null) killSwitch.shutdown()
  }

  private def evalSource(ds: Evaluator.DataSources): Source[Evaluator.MessageEnvelope, NotUsed] = {
    Source
      .repeat(ds)
      .throttle(1, 1.minute, 1, ThrottleMode.Shaping)
      .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
  }

  private def dataSources: List[Evaluator.DataSources] = {
    import scala.jdk.CollectionConverters.*
    val defaultStep = config.getDuration("iep.lwc.loadgen.step")
    config
      .getStringList("iep.lwc.loadgen.uris")
      .asScala
      .zipWithIndex
      .map {
        case (uri, i) =>
          val step = extractStep(uri).getOrElse(defaultStep)
          val id = Strings.zeroPad(i, 6)
          new Evaluator.DataSource(id, step, uri)
      }
      .grouped(1000)
      .map { grp =>
        new Evaluator.DataSources(grp.toSet.asJava)
      }
      .toList
  }

  private def updateStats(envelope: Evaluator.MessageEnvelope): Unit = {
    val id = limiter(envelope.id())
    envelope.message() match {
      case tsm: TimeSeriesMessage => record(id, "timeseries", value(tsm), tsm.start)
      case msg: DiagnosticMessage => record(id, msg.typeName)
      case _                      => record(id, "unknown")
    }
  }

  private def value(tsm: TimeSeriesMessage): Double = {
    tsm.data match {
      case ArrayData(vs) if vs.nonEmpty => vs.sum
      case _                            => Double.NaN
    }
  }

  private def record(
    id: String,
    msgType: String,
    value: Double = Double.NaN,
    timestamp: Long = 0L
  ): Unit = {
    val resultMessages = registry
      .createId("loadgen.resultMessages")
      .withTags("id", id, "msgType", msgType)
    registry.counter(resultMessages).increment()

    if (!value.isNaN) {
      // Fractional amounts between 0 and 1 are more common for our use-cases than numbers
      // large enough to overflow the long when multiplied by 1M. Since the distribution summary
      // is primarily as a sanity check of the values when comparing one version to the next
      // this step avoids mapping a larger range of relevant values to 0 when converting to
      // a long.
      val longValue = (value * 1e6).toLong
      PercentileDistributionSummary
        .builder(registry)
        .withName("loadgen.valueDistribution")
        .withTags(resultMessages.tags())
        .build()
        .record(longValue)
    }

    if (value > 0L) {
      val age = registry.clock().wallTime() - timestamp
      PercentileTimer
        .builder(registry)
        .withName("loadgen.ageDistribution")
        .build()
        .record(age, TimeUnit.MILLISECONDS)
    }
  }
}

object LoadGenService {

  def extractStep(uri: String): Option[java.time.Duration] = {
    val result = Try {
      Uri(uri).query().get("step").map(Strings.parseDuration)
    }
    result.toOption.flatten
  }
}
