package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes

extension (string: Expr[String]) def toInt: LinearExpr[Int] = LinearExpr(Cast(string.underlying, DataTypes.IntegerType))

extension (string: ScalarExpr[String])
  def toInt: ScalarExpr[Int] = ScalarExpr(Cast(string.underlying, DataTypes.IntegerType))
