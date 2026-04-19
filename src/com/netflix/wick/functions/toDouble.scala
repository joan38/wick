package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes
import scala.annotation.targetName
import java.math.BigDecimal

extension (string: Expr[String])
  def toDouble: LinearExpr[Double] = LinearExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: ScalarExpr[String])
  def toDouble: ScalarExpr[Double] = ScalarExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: Expr[BigDecimal])
  @targetName("bigDecimalToDouble")
  def toDouble: LinearExpr[Double] = LinearExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: ScalarExpr[BigDecimal])
  @targetName("bigDecimalToDouble")
  def toDouble: ScalarExpr[Double] = ScalarExpr(Cast(string.underlying, DataTypes.DoubleType))
