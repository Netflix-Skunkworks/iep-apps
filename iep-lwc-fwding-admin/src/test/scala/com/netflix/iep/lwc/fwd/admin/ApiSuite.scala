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

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite

class ApiSuite
    extends FunSuite
    with ScalatestRouteTest
    with CwForwardingTestConfig
    with StrictLogging {

  val validations = new CwExprValidations(new ExprInterpreter(ConfigFactory.load()))

  val routes = RequestHandler.standardOptions(
    new Api(
      new SchemaValidation,
      validations,
    ).routes
  )

  test("Valid config") {

    val config = makeConfigString()()

    val postRequest = HttpRequest(
      HttpMethods.POST,
      uri = "/api/v1/check/cwf/foo_cluster_config",
      entity = HttpEntity(MediaTypes.`application/json`, config)
    )

    postRequest ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("Fail for an invalid config") {

    val config = makeConfigString()(
      atlasUri = """
                   | http://localhost/api/v1/graph?q=
                   |  nf.app,nq_android_api,:eq,
                   |  name,cgroup,:re,:and,
                   |  (,name,nf.asg,nf.account,),:by
                 """.stripMargin
    )

    val postRequest = HttpRequest(
      HttpMethods.POST,
      uri = "/api/v1/check/cwf/foo_cluster_config",
      entity = HttpEntity(MediaTypes.`application/json`, config)
    )

    postRequest ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(
        entityAs[String]
          .contains(
            s"IllegalArgumentException: By default allowing only grouping by " +
            s"${validations.defaultGroupingKeys}"
          )
      )
    }
  }
}
