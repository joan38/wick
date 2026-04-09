package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark.sql.Column

/** Returns an expression sorted in descending order.
  *
  * @param expr
  *   the column expression to sort
  * @tparam T
  *   the data type of the column, must be orderable
  * @return
  *   a new expression with descending sort order
  */
def desc[T: Orderable](expr: LinearExpr[T]): LinearExpr[T] = LinearExpr(new Column(expr.underlying).desc.expr)
