package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the value from `expr` associated with the maximum value of `by`.
  *
  * This function finds the row with the maximum value of the `by` expression and returns the corresponding value of the
  * `expr` expression for that row.
  *
  * @tparam V
  *   the type of the value expression
  * @tparam T
  *   the type of the ordering expression (must have an Orderable instance)
  * @param expr
  *   the expression whose value will be returned
  * @param by
  *   the expression used to determine the maximum
  * @return
  *   a scalar expression containing the value from `expr` where `by` is maximum
  *
  * @example
  *   {{{// Returns the name of the person with the highest salary maxBy(col("name"), col("salary"))}}}
  */
def maxBy[V, T: Orderable](expr: LinearExpr[V], by: LinearExpr[T]): ScalarExpr[V] = ScalarExpr(
  spark.sql.functions.max_by(new Column(expr.underlying), new Column(by.underlying)).expr
)
