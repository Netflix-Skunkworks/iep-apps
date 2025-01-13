/*
 * Copyright 2014-2025 Netflix, Inc.
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

import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.json.Json
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

  private def createMessage(
    level: String,
    message: List[ValidationResult],
    n: Int
  ): FailureMessage = {
    val failures = message.collect {
      case msg: ValidationResult.Fail => msg
    }
    // Limit encoding the tags to just the summary set
    val summary = failures.take(5).map { msg =>
      s"${msg.reason} (tags=${Json.encode(msg.tags)})"
    }
    new FailureMessage(level, n, summary)
  }

  def error(message: List[ValidationResult], n: Int): FailureMessage = {
    createMessage(DiagnosticMessage.Error, message, n)
  }

  def partial(message: List[ValidationResult], n: Int): FailureMessage = {
    createMessage("partial", message, n)
  }
}
