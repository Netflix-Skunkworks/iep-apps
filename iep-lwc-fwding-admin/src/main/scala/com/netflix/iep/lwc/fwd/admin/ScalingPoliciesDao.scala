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
package com.netflix.iep.lwc.fwd.admin
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.http.scaladsl.model.headers.`Accept-Encoding`
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.json.Json
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import scala.util.Failure
import scala.util.Success

trait ScalingPoliciesDao {
  def getScalingPolicies(): Flow[EddaEndpoint, List[ScalingPolicy], NotUsed]
}

class ScalingPoliciesDaoImpl @Inject()(
  config: Config,
  implicit val system: ActorSystem
) extends ScalingPoliciesDao
    with StrictLogging {

  private implicit val mat = ActorMaterializer()
  private implicit val ec = scala.concurrent.ExecutionContext.global

  private val ec2PoliciesUri = config.getString("iep.lwc.fwding-admin.ec2-policies-uri")
  private val cwAlarmsUri = config.getString("iep.lwc.fwding-admin.cw-alarms-uri")
  private val titusPoliciesUri = config.getString("iep.lwc.fwding-admin.titus-policies-uri")

  protected val client = Http().superPool[AccessLogger]()

  override def getScalingPolicies(): Flow[EddaEndpoint, List[ScalingPolicy], NotUsed] = {

    Flow[EddaEndpoint]
      .filter { eddaEndpoint =>
        val valid = eddaEndpoint.account.nonEmpty &&
          eddaEndpoint.region.nonEmpty &&
          eddaEndpoint.env.nonEmpty

        if (!valid) {
          logger.error(s"Invalid EddaEndpoint $eddaEndpoint")
        }

        valid
      }
      .flatMapConcat { eddaEndpoint =>
        Source
          .single(eddaEndpoint)
          .map(makeEddaUri(_, cwAlarmsUri))
          .via(doGet())
          .map(Json.decode[List[CwAlarm]](_))
          .map((eddaEndpoint, _))
      }
      .flatMapConcat {
        case (eddaEndpoint, alarms) =>
          Source
            .single(eddaEndpoint)
            .map(makeEddaUri(_, ec2PoliciesUri))
            .via(doGet())
            .map(Json.decode[List[Ec2ScalingPolicy]](_))
            .map(makeEc2ScalingPolicies(_, alarms))
            .map((eddaEndpoint, alarms, _))
      }
      .flatMapConcat {
        case (eddaEndpoint, alarms, ec2Policies) =>
          Source
            .single(eddaEndpoint)
            .map(makeEddaUri(_, titusPoliciesUri))
            .via(doGet())
            .map(Json.decode[List[TitusScalingPolicy]](_))
            .map(makeTitusScalingPolicies(_, alarms))
            .map((ec2Policies, _))
      }
      .map {
        case (ec2, titus) =>
          ec2 ++ titus
      }
  }

  def makeEc2ScalingPolicies(
    ec2Policies: List[Ec2ScalingPolicy],
    alarms: List[CwAlarm]
  ): List[ScalingPolicy] = {

    ec2Policies.flatMap { policy =>
      val a = policy.alarms.flatMap { alarm =>
        alarms.find(_.alarmName == alarm.alarmName)
      }

      a.map { alarm =>
        ScalingPolicy(
          policy.getPolicyId(),
          ScalingPolicy.Ec2,
          alarm.metricName,
          alarm.dimensions
        )
      }
    }

  }

  def makeTitusScalingPolicies(
    titusPolicies: List[TitusScalingPolicy],
    alarms: List[CwAlarm]
  ): List[ScalingPolicy] = {

    titusPolicies.flatMap { policy =>
      if (policy.isTargetPolicy()) {
        val spec = policy.getCustomMetricSpec()

        Some(
          ScalingPolicy(
            policy.getPolicyId(),
            ScalingPolicy.Titus,
            spec.metricName,
            spec.dimensions
          )
        )
      } else {
        val alarmName = s"${policy.jobId}/${policy.id.id}"
        val a = alarms
          .find(_.alarmName == alarmName)

        a.map { alarm =>
          ScalingPolicy(
            policy.getPolicyId(),
            ScalingPolicy.Titus,
            alarm.metricName,
            alarm.dimensions
          )
        }
      }
    }
  }

  def doGet(): Flow[Uri, String, NotUsed] = {
    Flow[Uri]
      .map(
        uri =>
          HttpRequest(HttpMethods.GET, uri).withHeaders(
            `Accept-Encoding`(HttpEncodings.gzip),
            Accept(MediaTypes.`application/json`)
          )
      )
      .map(r => r -> AccessLogger.newClientLogger("edda", r))
      .via(client)
      .map {
        case (result, accessLog) =>
          accessLog.complete(result)

          result match {
            case Failure(e) => logger.error("Request to Edda failed", e)
            case _          =>
          }

          result
      }
      .collect { case Success(r) => r }
      .via(StreamOps.map { (r, mat) =>
        if (r.status != StatusCodes.OK) {
          r.discardEntityBytes()(mat)
        }
        r
      })
      .filter(_.status == StatusCodes.OK)
      .map(Gzip.decodeMessage(_))
      .mapAsync[String](1)(r => Unmarshal(r.entity).to[String])
  }

  def makeEddaUri(eddaEndpoint: EddaEndpoint, uriPattern: String): Uri = {
    Uri(uriPattern.format(eddaEndpoint.account, eddaEndpoint.region, eddaEndpoint.env))
  }
}

case class ScalingPolicy(
  policyId: String,
  policyType: String,
  metricName: String,
  dimensions: List[MetricDimension]
) {

  def matchMetric(metricInfo: FwdMetricInfo): Boolean = {
    metricInfo.name == metricName && metricInfo.dimensions == dimensions
      .map(d => (d.name, d.value))
      .toMap
  }
}
case class MetricDimension(name: String, value: String)

object ScalingPolicy {
  val Titus = "titus"
  val Ec2 = "ec2"
}

case class EddaEndpoint(account: String, region: String, env: String)

case class Ec2ScalingPolicy(policyName: String, alarms: List[CwAlarm]) {
  def getPolicyId(): String = policyName
}
case class CwAlarm(alarmName: String, metricName: String, dimensions: List[MetricDimension])

case class TitusScalingPolicy(jobId: String, id: Id, scalingPolicy: ScalingPolicyDetails) {
  def getPolicyId(): String = id.id

  def isTargetPolicy(): Boolean = {
    scalingPolicy.targetPolicyDescriptor.isDefined
  }

  def getCustomMetricSpec(): CustomMetricSpec = {
    scalingPolicy.targetPolicyDescriptor
      .map(_.customizedMetricSpecification)
      .getOrElse(throw new RuntimeException("targetPolicyDescriptor not found"))
  }

}
case class Id(id: String)
case class ScalingPolicyDetails(targetPolicyDescriptor: Option[TargetPolicyDescriptor])
case class TargetPolicyDescriptor(customizedMetricSpecification: CustomMetricSpec)
case class CustomMetricSpec(metricName: String, dimensions: List[MetricDimension])
