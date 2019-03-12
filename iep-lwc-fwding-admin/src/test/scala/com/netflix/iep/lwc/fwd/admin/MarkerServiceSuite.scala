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
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.iep.lwc.fwd.cw.ExpressionId
import com.netflix.iep.lwc.fwd.cw.ForwardingExpression
import com.netflix.iep.lwc.fwd.cw.FwdMetricInfo
import com.netflix.iep.lwc.fwd.cw.Report
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MarkerServiceSuite extends FunSuite {

  import MarkerServiceImpl._
  import ExpressionDetails._

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  test("Read ExpressionDetails using a dedicated dispatcher") {
    val exprDetailsDao = new ExpressionDetailsDaoTestImpl

    val report = Report(
      1551820461000L,
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
    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def read(id: ExpressionId): Option[ExpressionDetails] = {
        if (id.key == "c1") {
          throw new Exception("Read failed. Test error")
        }
        None
      }
    }

    val reports = List(
      Report(
        1551820461000L,
        ExpressionId("c1", ForwardingExpression("", "", None, "")),
        None,
        None
      ),
      Report(
        1551820461000L,
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

  test("Make ExpressionDetails from a new Report with data") {
    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      Some(FwdMetricInfo("", "", "", Map.empty[String, String])),
      None
    )

    val expected =
      ExpressionDetails(
        report.id,
        report.timestamp,
        report.metric,
        report.error,
        Map.empty[String, Long],
        None
      )
    val actual = toExprDetails(report, None)

    assert(actual === expected)
  }

  test("Make ExpressionDetails from a new Report with no data") {
    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )

    val expected =
      ExpressionDetails(
        report.id,
        report.timestamp,
        report.metric,
        report.error,
        Map(NoDataFoundEvent -> 1551820461000L),
        None
      )
    val actual = toExprDetails(report, None)

    assert(actual === expected)
  }

  test("Make ExpressionDetails from a Report with no data & prev Report with data") {
    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val prevExprDetails = ExpressionDetails(
      report.id,
      1551820341000L,
      report.metric,
      report.error,
      Map.empty[String, Long],
      None
    )

    val expected =
      ExpressionDetails(
        report.id,
        report.timestamp,
        report.metric,
        report.error,
        Map(NoDataFoundEvent -> 1551820461000L),
        None
      )
    val actual = toExprDetails(report, Some(prevExprDetails))
    assert(actual === expected)
  }

  test("Make ExpressionDetails from a Report with no data & prev Report with no data") {
    val report = Report(
      1551820461000L,
      ExpressionId("c1", ForwardingExpression("", "", None, "")),
      None,
      None
    )
    val prevExprDetails = ExpressionDetails(
      report.id,
      1551820341000L,
      report.metric,
      report.error,
      Map(NoDataFoundEvent -> 1551820341000L),
      None
    )

    val expected =
      ExpressionDetails(
        report.id,
        report.timestamp,
        report.metric,
        report.error,
        Map(NoDataFoundEvent -> 1551820341000L),
        None
      )
    val actual = toExprDetails(report, Some(prevExprDetails))
    assert(actual === expected)
  }

  test("Save ExpressionDetails using a dedicated dispatcher") {
    val saved = List.newBuilder[ExpressionDetails]

    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def save(exprDetails: ExpressionDetails): Unit = {
        saved += exprDetails
      }
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
      Map.empty[String, Long],
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
    val exprDetailsDao = new ExpressionDetailsDaoTestImpl {
      override def save(exprDetails: ExpressionDetails): Unit = {
        throw new Exception("Save failed. Test error")
      }
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
      Map.empty[String, Long],
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
