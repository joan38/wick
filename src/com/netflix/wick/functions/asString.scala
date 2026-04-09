package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.Cast
import com.netflix.wick.column.Numeric
import org.apache.spark.sql.types.DataTypes

extension [A <: Numeric | Null](expr: Expr[A])
  def asString: LinearExpr[String] = LinearExpr(Cast(expr.underlying, DataTypes.StringType))

extension [A <: Numeric | Null](expr: ScalarExpr[A])
  def asString: ScalarExpr[String] = ScalarExpr(Cast(expr.underlying, DataTypes.StringType))
