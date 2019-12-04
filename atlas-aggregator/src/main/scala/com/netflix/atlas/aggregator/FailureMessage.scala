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
package com.netflix.atlas.aggregator

import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.json.JsonSupport

/**
  * Message returned to the user if some of the datapoints sent fail validation rules.
  *
  * @param `type`
  *     Indicates whether the payload was an entirely or just partially impacted. If
  *     all datapoints failed, then it will return an error.
  * @param errorCount
  *     The number of failed datapoints in the payload.
  * @param message
  *     Sampled set of failure messages to help the user debug.
  */
case class FailureMessage(`type`: String, errorCount: Int, message: List[String])
    extends JsonSupport {

  def typeName: String = `type`
}

object FailureMessage {

  def error(message: List[ValidationResult], n: Int): FailureMessage = {
    val failures = message.collect { case ValidationResult.Fail(_, reason) => reason }
    new FailureMessage(DiagnosticMessage.Error, n, failures.take(5))
  }

  def partial(message: List[ValidationResult], n: Int): FailureMessage = {
    val failures = message.collect { case ValidationResult.Fail(_, reason) => reason }
    new FailureMessage("partial", n, failures.take(5))
  }
}
