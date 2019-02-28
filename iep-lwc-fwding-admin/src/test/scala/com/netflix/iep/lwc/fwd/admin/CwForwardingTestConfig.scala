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
import com.netflix.iep.lwc.fwd.cw.ClusterConfig
import com.netflix.iep.lwc.fwd.cw.ForwardingDimension
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression

trait CwForwardingTestConfig {

  def makeConfig(
    email: String = "app-oncall@netflix.com",
    metricName: String = "$(name)",
    atlasUri: String = """
                      | http://localhost/api/v1/graph?q=
                      |  nf.app,foo_app1,:eq,
                      |  name,nodejs.cpuUsage,:eq,:and,
                      |  :node-avg,
                      |  (,nf.account,nf.asg,),:by
                    """.stripMargin,
    dimensions: List[ForwardingDimension] = List(
      ForwardingDimension("AutoScalingGroupName", "$(nf.asg)")
    ),
    account: String = "$(nf.account)",
    region: Option[String] = Some("$(nf.region)"),
    checksToSkip: List[String] = List.empty[String],
  ): ClusterConfig = {
    new ClusterConfig(
      email,
      List(
        ForwardingExpression(
          atlasUri
            .filterNot(_.isWhitespace)
            .replace("\n", ""),
          account,
          region,
          metricName,
          dimensions,
        )
      ),
      checksToSkip
    )
  }

  def makeConfigString(
    dimensionName: String = "AutoScalingGroupName",
    dimensionValue: String = "$(nf.asg)"
  )(
    email: String = "app-oncall@netflix.com",
    metricName: String = "$(name)",
    atlasUri: String = """
                      | http://localhost/api/v1/graph?q=
                      |  nf.app,foo_app,:eq,
                      |  name,nodejs.cpuUsage,:eq,:and,
                      |  :node-avg,
                      |  (,nf.account,nf.asg,),:by
                    """.stripMargin,
    dimensions: String = s"""
         | [
         |   {
         |     "name":"$dimensionName",  
         |     "value":"$dimensionValue"
         |   }
         | ]
      """.stripMargin,
    account: String = "$(nf.account)",
    region: String = "$(nf.region)",
    checksToSkip: String = """["AsgGrouping"]"""
  ): String = {

    val uri = atlasUri
      .filterNot(_.isWhitespace)
      .replace("\n", "")

    s"""
       |{
       |  "email": "$email",
       |  "expressions": [
       |    {
       |      "metricName": "$metricName",
       |      "atlasUri": "$uri",
       |      "dimensions": $dimensions,
       |      "account": "$account",
       |      "region": "$region"
       |    }
       |  ],
       |  "checksToSkip": $checksToSkip
       |}
      """.stripMargin
  }

}
