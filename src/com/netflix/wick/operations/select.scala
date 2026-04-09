package com.netflix.wick
package operations

import com.netflix.wick.column.Columns
import org.apache.spark.sql.Column
import com.netflix.wick.util.NamedTuples.InverseMap
import scala.NamedTuple.AnyNamedTuple

extension [T](dataSeq: DataSeq[T])
  def select[Cols <: AnyNamedTuple: Columns as columns](on: DataSeq.Ref[T] => Cols): DataSeq[Selected[Cols]] =
    val namedExpressions = columns(on(DataSeq.Ref(Some(dataSeq.dataFrame))))
    DataSeq(dataSeq.dataFrame.select(namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))*))

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def select[Cols <: AnyNamedTuple: Columns as columns](
      on: Tuple.Map[Joined, DataSeq.Ref] => Cols
  ): DataSeq[Selected[Cols]] =
    val namedExpressions = columns(on(joinedDataSeq.refs))
    DataSeq(joinedDataSeq.dataFrame.select(namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))*))

type Selected[Cols <: AnyNamedTuple] = InverseMap[Cols, Expr]
