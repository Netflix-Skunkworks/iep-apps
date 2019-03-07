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

import java.util.concurrent.TimeUnit._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.iep.lwc.fwd.cw.Report
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MarkerServiceSuite extends FunSuite {

  import MarkerServiceImpl._

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  test("Read ExpressionDetails using a dedicated dispatcher") {
    val exprDetailsDao = new ExpressionDetailsDao {
      override def save(exprDetails: ExpressionDetails): Unit = {}
      override def read(id: ExpressionId): Option[ExpressionDetails] = None
    }

    val report = Report(
      System.currentTimeMillis(),
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )

    val future = Source
      .single(report)
      .via(readExprDetails(exprDetailsDao))
      .runWith(Sink.head)
    val actual = Await.result(future, Duration.Inf)

    assert(actual === ((report, None)))
  }

  test("Read errors should be filtered out from the stream") {
    val exprDetailsDao = new ExpressionDetailsDao {
      override def save(exprDetails: ExpressionDetails): Unit = {}
      override def read(id: ExpressionId): Option[ExpressionDetails] = {
        if (id.key == "c1") {
          throw new Exception("Read failed. Test error")
        }
        None
      }
    }

    val reports = List(
      Report(
        System.currentTimeMillis(),
        ExpressionId("c1", ForwardingExpression("", "", None, "")),
        None,
        None
      ),
      Report(
        System.currentTimeMillis(),
        ExpressionId("c2", ForwardingExpression("", "", None, "")),
        None,
        None
      )
    )

    val future = Source(reports)
      .via(readExprDetails(exprDetailsDao))
      .runWith(Sink.seq)
    val actual = Await.result(future, Duration.Inf)

    assert(actual === List(((reports.find(_.id.key == "c2").get, None))))
  }

  test("Make ExpressionDetails for a new Report") {
    val report = Report(
      System.currentTimeMillis(),
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )

    val expected =
      ExpressionDetails(report.id, report.timestamp, report.metric, report.error, 0, 0, None)
    val actual = toExprDetails(report, None)

    assert(actual === expected)
  }

  test("Update ExpressionDetails for the given report") {
    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val exprDetails = ExpressionDetails(
      report.id,
      report.timestamp - Duration(2, MINUTES).toMillis,
      report.metric,
      report.error,
      0,
      0,
      None
    )

    val expected =
      ExpressionDetails(
        report.id,
        report.timestamp,
        report.metric,
        report.error,
        2,
        0,
        None
      )
    val actual = toExprDetails(report, Some(exprDetails))
    assert(actual === expected)
  }

  test("Save ExpressionDetails using a dedicated dispatcher") {
    val saved = List.newBuilder[ExpressionDetails]

    val exprDetailsDao = new ExpressionDetailsDao {

      override def save(exprDetails: ExpressionDetails): Unit = {
        saved += exprDetails
      }
      override def read(id: ExpressionId): Option[ExpressionDetails] = None
    }

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val exprDetails = ExpressionDetails(
      report.id,
      report.timestamp,
      report.metric,
      report.error,
      0,
      0,
      None
    )

    val future = Source
      .single(exprDetails)
      .via(saveExprDetails(exprDetailsDao))
      .runWith(Sink.head)
    val result = Await.result(future, Duration.Inf)

    assert(result === NotUsed)
    assert(saved.result() === List(exprDetails))
  }

  test("Save errors should be filtered out") {
    val exprDetailsDao = new ExpressionDetailsDao {

      override def save(exprDetails: ExpressionDetails): Unit = {
        throw new Exception("Save failed. Test error")
      }
      override def read(id: ExpressionId): Option[ExpressionDetails] = None
    }

    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val exprDetails = ExpressionDetails(
      report.id,
      report.timestamp,
      report.metric,
      report.error,
      0,
      0,
      None
    )

    val future = Source
      .single(exprDetails)
      .via(saveExprDetails(exprDetailsDao))
      .runWith(Sink.headOption)
    val result = Await.result(future, Duration.Inf)

    assert(result === None)
  }
}
