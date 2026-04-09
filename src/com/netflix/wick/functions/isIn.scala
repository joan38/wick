package com.netflix.wick
package functions

import org.apache.spark.sql.Column

extension [T](expr: Expr[T])
  def isIn(values: T*): LinearExpr[Boolean] = LinearExpr(
    new Column(expr.underlying).isin(values*).expr
  )

extension [T](expr: ScalarExpr[T])
  def isIn(values: T*): ScalarExpr[Boolean] = ScalarExpr(
    new Column(expr.underlying).isin(values*).expr
  )
