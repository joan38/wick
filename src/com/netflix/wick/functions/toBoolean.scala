package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes

extension (string: Expr[String])
  def toBoolean: LinearExpr[Boolean] = LinearExpr(Cast(string.underlying, DataTypes.BooleanType))

extension (string: ScalarExpr[String])
  def toBoolean: ScalarExpr[Boolean] = ScalarExpr(Cast(string.underlying, DataTypes.BooleanType))
