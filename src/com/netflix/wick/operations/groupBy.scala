package com.netflix.wick
package operations

import com.netflix.wick.column.{Columns, IsScalarCols}
import org.apache.spark.sql.Column
import scala.NamedTuple.AnyNamedTuple

extension [T](dataSeq: DataSeq[T])
  def groupBy[Keys <: AnyNamedTuple: Columns as cols](on: DataSeq.Ref[T] => Keys): GroupedDataSeq[Keys, T] =
    val namedExpressions = cols(on(DataSeq.Ref(Some(dataSeq.dataFrame))))
    GroupedDataSeq(dataSeq.dataFrame.groupBy(namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))*))

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def groupBy[Keys <: AnyNamedTuple: Columns as cols](
      on: Tuple.Map[Joined, DataSeq.Ref] => Keys
  ): JoinedGroupedDataSeq[Keys, Joined] =
    val namedExpressions = cols(on(joinedDataSeq.refs))
    JoinedGroupedDataSeq(
      joinedDataSeq.dataFrame.groupBy(namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))*),
      joinedDataSeq.refs
    )

extension [T](dataSeq: DataSeq[T])
  inline def agg[Cols <: AnyNamedTuple: {Columns as cols, IsScalarCols}](
      on: DataSeq.Ref[T] => Cols
  ): DataSeq[Selected[Cols]] =
    val namedExpressions = cols(on(DataSeq.Ref(Some(dataSeq.dataFrame))))
    val ScalarExprs      = namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))
    DataSeq(dataSeq.dataFrame.agg(ScalarExprs.head, ScalarExprs.tail*))
