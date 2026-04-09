package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes

extension (string: Expr[String]) def toLong: LinearExpr[Long] = LinearExpr(Cast(string.underlying, DataTypes.LongType))

extension (string: ScalarExpr[String])
  def toLong: ScalarExpr[Long] = ScalarExpr(Cast(string.underlying, DataTypes.LongType))
