package com.netflix.wick
package functions

import com.netflix.wick.column.Numeric
import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the average of all values in an expression.
  *
  * @param expr
  *   the expression to compute the average for
  * @tparam T
  *   the numeric data type of the expression
  * @return
  *   a scalar expression containing the average value
  */
def avg[T <: Numeric | Null](expr: Expr[T]): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.avg(new Column(expr.underlying)).expr
)
