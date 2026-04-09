package com.netflix.wick
package operations

import scala.concurrent.duration.Duration
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Alias

extension [T](dataSeq: DataSeq[T])
  def withWatermark[E](eventTimeCol: DataSeq.Ref[T] => Expr[E], delayThreshold: Duration): DataSeq[T] =
    val eventTimeExpr = eventTimeCol(DataSeq.Ref(Some(dataSeq.dataFrame)))
    val columnName    = eventTimeExpr.underlying match
      case attr: AttributeReference => attr.name
      case alias: Alias             => alias.name
      case _                        =>
        throw IllegalArgumentException(
          s"withWatermark requires a direct column reference. Found: ${eventTimeExpr.underlying.prettyName}"
        )

    DataSeq(dataSeq.dataFrame.withWatermark(columnName, delayThreshold.toString))
