package com.netflix.wick
package operations

import org.apache.spark.sql.Column

extension [T](dataSeq: DataSeq[T])
  def filter(condition: DataSeq.Ref[T] => Expr[Boolean]): DataSeq[T] =
    val filterCondition = condition(DataSeq.Ref(Some(dataSeq.dataFrame)))
    DataSeq(dataSeq.dataFrame.filter(new Column(filterCondition.underlying)))

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def filter(condition: Tuple.Map[Joined, DataSeq.Ref] => Expr[Boolean]): JoinedDataSeq[Joined] =
    val filterCondition = condition(joinedDataSeq.refs)
    JoinedDataSeq(joinedDataSeq.dataFrame.filter(new Column(filterCondition.underlying)), joinedDataSeq.refs)
