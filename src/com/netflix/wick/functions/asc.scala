package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark.sql.Column

/** Returns an expression sorted in ascending order.
  *
  * @param expr
  *   the expression to sort
  * @tparam T
  *   the data type of the expression, must be orderable
  * @return
  *   a new expression with ascending sort order
  */
def asc[T: Orderable](expr: LinearExpr[T]): LinearExpr[T] = LinearExpr(new Column(expr.underlying).asc.expr)
