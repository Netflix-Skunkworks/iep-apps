/*
 * Copyright 2014-2023 Netflix, Inc.
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

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.pattern.AskTimeoutException
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.testkit.ImplicitSender
import org.apache.pekko.testkit.TestActorRef
import org.apache.pekko.testkit.TestKitBase
import org.apache.pekko.util.Timeout
import com.netflix.iep.lwc.fwd.admin.ScalingPolicies.GetCache
import com.netflix.iep.lwc.fwd.admin.ScalingPolicies.GetScalingPolicy
import com.netflix.iep.lwc.fwd.admin.ScalingPolicies.RefreshCache
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ScalingPoliciesSuite extends FunSuite with TestKitBase with ImplicitSender {

  implicit val system: ActorSystem = ActorSystem()
  implicit val timeout: Timeout = testKitSettings.DefaultTimeout

  private val config = ConfigFactory.load()

  test("Get cached Scaling policy") {
    val ec2Policy1 = ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil)
    val data = Map(EddaEndpoint("123", "us-east-1", "local") -> List(ec2Policy1))

    val scalingPolicies = system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDaoTestImpl(Map.empty[EddaEndpoint, List[ScalingPolicy]]),
          data
        )
      )
    )

    val future = scalingPolicies ? GetScalingPolicy(
      FwdMetricInfo("us-east-1", "123", "metric1", Map.empty[String, String])
    )
    val actual = Await.result(future.mapTo[Option[ScalingPolicy]], Duration.Inf)
    val expected = Some(ec2Policy1)
    assertEquals(actual, expected)
  }

  test("Lookup Scaling policy") {
    val ec2Policy1 = ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil)
    val data = Map(EddaEndpoint("123", "us-east-1", "local") -> List(ec2Policy1))
    val scalingPolicies = system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDaoTestImpl(policies = data)
        )
      )
    )

    var future = scalingPolicies ? GetScalingPolicy(
      FwdMetricInfo("us-east-1", "123", "metric1", Map.empty[String, String])
    )
    val actual = Await.result(future.mapTo[Option[ScalingPolicy]], Duration.Inf)

    val expected = Some(ec2Policy1)
    assertEquals(actual, expected)

    future = scalingPolicies ? GetCache
    val cachedScalingPolicies =
      Await.result(future.mapTo[Map[EddaEndpoint, List[ScalingPolicy]]], Duration.Inf)
    assertEquals(cachedScalingPolicies, data)
  }

  test("Timeout when dao fails to lookup Edda") {
    val scalingPolicies = system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDao {
            override def getScalingPolicies: Flow[EddaEndpoint, List[ScalingPolicy], NotUsed] = {
              Flow[EddaEndpoint]
                .filter(_ => false)
                .map(_ => List.empty[ScalingPolicy])
            }
          }
        )
      )
    )
    val future = scalingPolicies ? GetScalingPolicy(
      FwdMetricInfo("us-east-1", "123", "metric1", Map.empty[String, String])
    )

    intercept[AskTimeoutException](
      Await.result(future.mapTo[Option[ScalingPolicy]], Duration.Inf)
    )
  }

  test("Refresh cache") {
    val ec2Policy1 = ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil)
    val ec2Policy2 = ScalingPolicy("ec2Policy2", ScalingPolicy.Ec2, "metric2", Nil)

    val eddaEndpoint = EddaEndpoint("123", "us-east-1", "local")
    val cache = Map(eddaEndpoint -> List(ec2Policy1))
    val data = Map(eddaEndpoint -> List(ec2Policy1, ec2Policy2))

    val scalingPolicies = system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDaoTestImpl(policies = data),
          cache
        )
      )
    )

    var future = scalingPolicies ? RefreshCache
    Await.ready(future, Duration.Inf)

    future = scalingPolicies ? GetCache
    val actual = Await.result(future.mapTo[Map[EddaEndpoint, List[ScalingPolicy]]], Duration.Inf)

    assertEquals(actual, data)
  }

  test("Dao failure during cache refresh") {
    val ec2Policy1 = ScalingPolicy("ec2Policy1", ScalingPolicy.Ec2, "metric1", Nil)
    val cache = Map(EddaEndpoint("123", "us-east-1", "local") -> List(ec2Policy1))

    val scalingPolicies = system.actorOf(
      Props[ScalingPoliciesTestImpl](
        new ScalingPoliciesTestImpl(
          config,
          new ScalingPoliciesDao {
            override def getScalingPolicies: Flow[EddaEndpoint, List[ScalingPolicy], NotUsed] = {
              Flow[EddaEndpoint]
                .filter(_ => false)
                .map(_ => List.empty[ScalingPolicy])
            }
          },
          cache
        )
      )
    )

    var future = scalingPolicies ? RefreshCache
    Await.ready(future, Duration.Inf)

    future = scalingPolicies ? GetCache
    val actual = Await.result(future.mapTo[Map[EddaEndpoint, List[ScalingPolicy]]], Duration.Inf)
    assertEquals(actual, cache)
  }

}

class ScalingPoliciesMethodsSuite extends FunSuite with TestKitBase {

  implicit val system: ActorSystem = ActorSystem()

  private val config = ConfigFactory.load()

  test("Get env mapping for the given account") {
    val scalingPolicies = TestActorRef(
      new ScalingPoliciesTestImpl(
        config,
        new ScalingPoliciesDaoTestImpl(Map.empty[EddaEndpoint, List[ScalingPolicy]]),
        Map.empty[EddaEndpoint, List[ScalingPolicy]]
      )
    )

    val expected = "local"
    val actual = scalingPolicies.underlyingActor.getEnv("123")
    assertEquals(actual, expected)
  }

  test("Get default env when a account is not mapped") {
    val scalingPolicies = TestActorRef(
      new ScalingPoliciesTestImpl(
        config,
        new ScalingPoliciesDaoTestImpl(Map.empty[EddaEndpoint, List[ScalingPolicy]]),
        Map.empty[EddaEndpoint, List[ScalingPolicy]]
      )
    )

    val expected = "test"
    val actual = scalingPolicies.underlyingActor.getEnv("456")
    assertEquals(actual, expected)
  }
}
