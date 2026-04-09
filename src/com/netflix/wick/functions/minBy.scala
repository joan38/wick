package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the value from `expr` associated with the minimum value of `by`.
  *
  * This function finds the row with the minimum value of the `by` expression and returns the corresponding value of the
  * `expr` expression for that row.
  *
  * @tparam V
  *   the type of the value expression
  * @tparam T
  *   the type of the ordering expression (must have an Orderable instance)
  * @param expr
  *   the expression whose value will be returned
  * @param by
  *   the expression used to determine the minimum
  * @return
  *   a scalar expression containing the value from `expr` where `by` is minimum
  *
  * @example
  *   {{{// Returns the name of the person with the lowest salary minBy(col("name"), col("salary"))}}}
  */
def minBy[V, T: Orderable](expr: LinearExpr[V], by: LinearExpr[T]): ScalarExpr[V] = ScalarExpr(
  spark.sql.functions.min_by(new Column(expr.underlying), new Column(by.underlying)).expr
)
