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
package com.netflix.iep.lwc.fwd.admin

import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.springframework.beans.factory.InitializingBean

import scala.concurrent.ExecutionContext
import scala.util.Try

class ExpressionSanityChecker(
  config: Config,
  expressionDetailsDao: ExpressionDetailsDao,
  cwExprValidations: CwExprValidations,
  purger: Purger,
  implicit val system: ActorSystem
) extends InitializingBean
    with StrictLogging {

  private val deleteInvalid =
    config.getBoolean("iep.lwc.fwding-admin.sanity-check.delete-invalid")

  override def afterPropertiesSet(): Unit = {
    implicit val ec: ExecutionContext = system.dispatcher
    scala.concurrent.Future(checkAll()).recover {
      case t => logger.error("expression sanity check failed unexpectedly", t)
    }
  }

  private def checkAll(): Unit = {
    val allIds = expressionDetailsDao.scan()
    logger.info(s"expression sanity check starting for ${allIds.size} expressions")

    val invalid = allIds.flatMap { id =>
      validateOne(id) match {
        case Some(reason) =>
          logger.warn(
            s"invalid expression flagged key=${id.key} atlasUri=${id.expression.atlasUri} reason=$reason"
          )
          Some(id)
        case None =>
          None
      }
    }

    if (invalid.isEmpty) {
      logger.info("expression sanity check complete: all expressions are valid")
    } else {
      logger.warn(
        s"expression sanity check found ${invalid.size} invalid out of ${allIds.size} expressions"
      )
      if (deleteInvalid) {
        logger.warn(s"deleting ${invalid.size} invalid expressions (delete-invalid=true)")
        purger.purgeInvalid(invalid)
      }
    }
  }

  private def validateOne(id: ExpressionId): Option[String] = {
    Try {
      cwExprValidations.validStreamingExpr(id.expression)
      val styleExprs = cwExprValidations.eval(id.expression.atlasUri)
      cwExprValidations.singleExpression(styleExprs)
      cwExprValidations.allGroupingsMapped(id.expression, styleExprs)
      cwExprValidations.variablesSubstitution(id.expression, styleExprs)
    }.toEither.left.toOption.map(_.getMessage)
  }
}
