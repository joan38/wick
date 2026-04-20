package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.DecimalType
import scala.annotation.targetName
import java.math.BigDecimal

extension (decimal: Expr[BigDecimal])
  /** Casts a BigDecimal expression to a Decimal of the given precision and scale.
    *
    * Values that do not fit the target precision produce null.
    *
    * @param precision
    *   the total number of significant digits
    * @param scale
    *   the number of digits to the right of the decimal point
    * @return
    *   a LinearExpr[BigDecimal] with the requested precision and scale
    */
  def toDecimal(precision: Int, scale: Int): LinearExpr[BigDecimal] =
    LinearExpr(Cast(decimal.underlying, DecimalType(precision, scale)))

extension (decimal: ScalarExpr[BigDecimal])
  /** Casts a BigDecimal scalar expression to a Decimal of the given precision and scale.
    *
    * @param precision
    *   the total number of significant digits
    * @param scale
    *   the number of digits to the right of the decimal point
    * @return
    *   a ScalarExpr[BigDecimal] with the requested precision and scale
    */
  def toDecimal(precision: Int, scale: Int): ScalarExpr[BigDecimal] =
    ScalarExpr(Cast(decimal.underlying, DecimalType(precision, scale)))

extension (double: Expr[Double])
  /** Casts a Double expression to a Decimal of the given precision and scale.
    *
    * @param precision
    *   the total number of significant digits
    * @param scale
    *   the number of digits to the right of the decimal point
    * @return
    *   a LinearExpr[BigDecimal] with the requested precision and scale
    */
  @targetName("doubleToDecimal")
  def toDecimal(precision: Int, scale: Int): LinearExpr[BigDecimal] =
    LinearExpr(Cast(double.underlying, DecimalType(precision, scale)))

extension (double: ScalarExpr[Double])
  /** Casts a Double scalar expression to a Decimal of the given precision and scale.
    *
    * @param precision
    *   the total number of significant digits
    * @param scale
    *   the number of digits to the right of the decimal point
    * @return
    *   a ScalarExpr[BigDecimal] with the requested precision and scale
    */
  @targetName("doubleToDecimal")
  def toDecimal(precision: Int, scale: Int): ScalarExpr[BigDecimal] =
    ScalarExpr(Cast(double.underlying, DecimalType(precision, scale)))
