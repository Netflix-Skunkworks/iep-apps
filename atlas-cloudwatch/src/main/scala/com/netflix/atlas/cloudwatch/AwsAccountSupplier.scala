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
package com.netflix.atlas.cloudwatch

import software.amazon.awssdk.regions.Region

import scala.concurrent.Future

/**
  * Interface for supplying the list of accounts, regions and namespaces to poll for CloudWatch metrics.
  */
trait AwsAccountSupplier {

  /**
    * The map of accounts to regions to namespaces for polling. The final set is the namespace list in
    * the format of CloudWatch, e.g. "AWS/EC2" or "AWS/ECS".
    * @return
    *     The non-null map of account IDs to poll for CloudWatch metrics along with the regions they
    *     operate in and namespaces to poll.
    */
  def accounts: Map[String, Map[Region, Set[String]]]

}
