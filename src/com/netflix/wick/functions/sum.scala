package com.netflix.wick
package functions

import com.netflix.wick.column.Numeric
import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the sum of all values in an expression.
  *
  * @param expr
  *   the expression to compute the sum for
  * @tparam T
  *   the numeric data type of the expression
  * @return
  *   a scalar expression containing the sum of all values
  */
def sum[T <: Numeric | Null](expr: LinearExpr[T]): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.sum(new Column(expr.underlying)).expr
)
