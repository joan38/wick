package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes
import scala.annotation.targetName
import com.netflix.wick.column.Numeric

extension (string: Expr[String]) def toLong: LinearExpr[Long] = LinearExpr(Cast(string.underlying, DataTypes.LongType))

extension (string: ScalarExpr[String])
  def toLong: ScalarExpr[Long] = ScalarExpr(Cast(string.underlying, DataTypes.LongType))

extension (numeric: Expr[Numeric])
  @targetName("numericToLong")
  def toLong: LinearExpr[Long] = LinearExpr(Cast(numeric.underlying, DataTypes.LongType))

extension (numeric: ScalarExpr[Numeric])
  @targetName("numericToLong")
  def toLong: ScalarExpr[Long] = ScalarExpr(Cast(numeric.underlying, DataTypes.LongType))
