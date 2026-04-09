package com.netflix.wick
package operations

import com.netflix.wick.column.*
import org.apache.spark.sql.Column
import scala.NamedTuple.NamedTuple
import scala.NamedTuple.AnyNamedTuple

extension [T](dataSeq: DataSeq[T])
  inline def withColumns[Cols <: AnyNamedTuple: Columns as cols](
      on: DataSeq.Ref[T] => Cols
  ): DataSeq[WithColumns[T, Cols]] =
    val namedExpressions = cols(on(DataSeq.Ref(Some(dataSeq.dataFrame))))
    DataSeq(dataSeq.dataFrame.withColumns(namedExpressions.map(ne => ne.name -> new Column(ne.expr.underlying)).toMap))

type WithColumns[T, Cols <: AnyNamedTuple] = NamedTuple.Concat[
  NamedTuple.From[T],
  NamedTuple[NamedTuple.Names[Cols], Tuple.InverseMap[NamedTuple.DropNames[Cols], Expr]]
]
