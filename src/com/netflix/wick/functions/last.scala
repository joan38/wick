package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the last value in an expression.
  *
  * @param expr
  *   the expression to get the last value from
  * @tparam T
  *   the data type of the expression
  * @return
  *   a scalar expression containing the last value
  */
def last[T](expr: Expr[T]): ScalarExpr[T] =
  ScalarExpr(spark.sql.functions.last(new Column(expr.underlying)).expr)

/** Returns the last value in an expression, with option to ignore nulls.
  *
  * @param expr
  *   the expression to get the last value from
  * @param ignoreNulls
  *   whether to ignore null values when finding the last value
  * @tparam T
  *   the data type of the expression
  * @return
  *   a scalar expression containing the last value
  */
def last[T](expr: Expr[T | Null], ignoreNulls: Boolean = false): ScalarExpr[Option[T]] =
  ScalarExpr(spark.sql.functions.last(new Column(expr.underlying), ignoreNulls).expr)
