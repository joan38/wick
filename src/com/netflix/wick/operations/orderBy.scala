package com.netflix.wick
package operations

import org.apache.spark.sql.Column
import com.netflix.wick.column.Expressions
import com.netflix.wick.column.Orderable

extension [T](dataSeq: DataSeq[T])
  def orderBy[Cols: {Expressions as expressions, Orderable}](on: DataSeq.Ref[T] => Cols): DataSeq[T] =
    val sortOn = expressions(on(DataSeq.Ref(Some(dataSeq.dataFrame))))
    DataSeq(dataSeq.dataFrame.sort(sortOn.map(col => new Column(col.underlying))*))

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def orderBy[Cols: {Expressions as expressions, Orderable}](
      on: Tuple.Map[Joined, DataSeq.Ref] => Cols
  ): JoinedDataSeq[Joined] =
    val sortOn = expressions(on(joinedDataSeq.refs))
    JoinedDataSeq(joinedDataSeq.dataFrame.sort(sortOn.map(col => new Column(col.underlying))*), joinedDataSeq.refs)
