package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes

extension (string: Expr[String])
  def toDouble: LinearExpr[Double] = LinearExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: ScalarExpr[String])
  def toDouble: ScalarExpr[Double] = ScalarExpr(Cast(string.underlying, DataTypes.DoubleType))
