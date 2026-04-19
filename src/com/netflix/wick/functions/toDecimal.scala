package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.DecimalType
import scala.annotation.targetName
import java.math.BigDecimal

extension (decimal: Expr[BigDecimal])
  def toDecimal(precision: Int, scale: Int): LinearExpr[BigDecimal] =
    LinearExpr(Cast(decimal.underlying, DecimalType(precision, scale)))

extension (decimal: ScalarExpr[BigDecimal])
  def toDecimal(precision: Int, scale: Int): ScalarExpr[BigDecimal] =
    ScalarExpr(Cast(decimal.underlying, DecimalType(precision, scale)))

extension (double: Expr[Double])
  @targetName("doubleToDecimal")
  def toDecimal(precision: Int, scale: Int): LinearExpr[BigDecimal] =
    LinearExpr(Cast(double.underlying, DecimalType(precision, scale)))

extension (double: ScalarExpr[Double])
  @targetName("doubleToDecimal")
  def toDecimal(precision: Int, scale: Int): ScalarExpr[BigDecimal] =
    ScalarExpr(Cast(double.underlying, DecimalType(precision, scale)))
