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
package com.netflix.iep.lwc.fwd.admin

import org.apache.pekko.http.scaladsl.model.Uri
import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.Config

class ExprInterpreter(config: Config) {

  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  def eval(atlasUri: String): List[StyleExpr] = {
    eval(Uri(atlasUri))
  }

  def eval(uri: Uri): List[StyleExpr] = {
    val expr = uri.query().get("q").getOrElse {
      throw new IllegalArgumentException(
        s"missing required URI parameter `q`: $uri"
      )
    }

    doEval(expr)
  }

  def doEval(expr: String): List[StyleExpr] = {
    interpreter.execute(expr).stack.map {
      case ModelExtractors.PresentationType(t) => t
      case v                                   => throw new MatchError(v)
    }
  }
}
