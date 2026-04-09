package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the first value in an expression.
  *
  * @param expr
  *   the expression to get the first value from
  * @tparam T
  *   the data type of the expression
  * @return
  *   a scalar expression containing the first value
  */
def first[T](expr: LinearExpr[T]): ScalarExpr[T] =
  ScalarExpr(spark.sql.functions.first(new Column(expr.underlying)).expr)

/** Returns the first value in an expression, with option to ignore nulls.
  *
  * @param expr
  *   the expression to get the first value from
  * @param ignoreNulls
  *   whether to ignore null values when finding the first value
  * @tparam T
  *   the data type of the expression
  * @return
  *   a scalar expression containing the first value
  */
def first[T](expr: LinearExpr[T | Null], ignoreNulls: Boolean = false): ScalarExpr[Option[T]] =
  ScalarExpr(spark.sql.functions.first(new Column(expr.underlying), ignoreNulls).expr)
