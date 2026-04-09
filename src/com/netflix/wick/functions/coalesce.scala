package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column
import scala.annotation.targetName

/** Returns the first column that is not null, or null if all inputs are null.
  *
  * For example, `coalesce(a, b, c)` will return a if a is not null, or b if a is null and b is not null, or c if both a
  * and b are null but c is not null.
  *
  * It is recommended to use .orElse() instead since it can track if the expression is still nullable or not with the
  * -Yexplicit-nulls Scala compiler option.
  */
@deprecated("Use .orElse() instead since it can track if the column is still nullable or not")
def coalesce[T](expr: Expr[T | Null], fallbacks: (Expr[T] | Expr[T | Null])*): LinearExpr[T] =
  LinearExpr(spark.sql.functions.coalesce((expr +: fallbacks).map(expr => new Column(expr.underlying))*).expr)

/** Returns the first column that is not null, or null if all inputs are null.
  *
  * For example, `coalesce(a, b, c)` will return a if a is not null, or b if a is null and b is not null, or c if both a
  * and b are null but c is not null.
  *
  * It is recommended to use .orElse() instead since it can track if the expression is still nullable or not with the
  * -Yexplicit-nulls Scala compiler option.
  */
@deprecated("Use .orElse() instead since it can track if the column is still nullable or not")
@targetName("coalesceScalar")
def coalesce[T](expr: ScalarExpr[T | Null], fallbacks: (ScalarExpr[T] | ScalarExpr[T | Null])*): ScalarExpr[T] =
  ScalarExpr(spark.sql.functions.coalesce((expr +: fallbacks).map(expr => new Column(expr.underlying))*).expr)
