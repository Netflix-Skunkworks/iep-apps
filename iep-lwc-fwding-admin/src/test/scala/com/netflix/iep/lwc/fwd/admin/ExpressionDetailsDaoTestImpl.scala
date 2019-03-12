package com.netflix.iep.lwc.fwd.admin
import com.netflix.iep.lwc.fwd.cw.ExpressionId

class ExpressionDetailsDaoTestImpl extends ExpressionDetailsDao {
  override def save(exprDetails: ExpressionDetails): Unit = {}
  override def read(id: ExpressionId): Option[ExpressionDetails] = None
  override def scan(): List[ExpressionId] = Nil
  override def queryPurgeEligible(now: Long, events: List[String]): List[ExpressionId] = Nil
  override def delete(id: ExpressionId): Unit = {}
}
