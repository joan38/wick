package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes
import scala.annotation.targetName
import java.math.BigDecimal

extension (string: Expr[String])
  /** Casts a string expression to a Double.
    *
    * Strings that cannot be parsed as doubles produce null.
    *
    * @return
    *   a LinearExpr[Double] containing the parsed double
    */
  def toDouble: LinearExpr[Double] = LinearExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: ScalarExpr[String])
  /** Casts a scalar string expression to a Double.
    *
    * @return
    *   a ScalarExpr[Double] containing the parsed double
    */
  def toDouble: ScalarExpr[Double] = ScalarExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: Expr[BigDecimal])
  /** Casts a BigDecimal expression to a Double.
    *
    * May lose precision since Double has limited mantissa bits.
    *
    * @return
    *   a LinearExpr[Double] containing the converted value
    */
  @targetName("bigDecimalToDouble")
  def toDouble: LinearExpr[Double] = LinearExpr(Cast(string.underlying, DataTypes.DoubleType))

extension (string: ScalarExpr[BigDecimal])
  /** Casts a BigDecimal scalar expression to a Double.
    *
    * May lose precision since Double has limited mantissa bits.
    *
    * @return
    *   a ScalarExpr[Double] containing the converted value
    */
  @targetName("bigDecimalToDouble")
  def toDouble: ScalarExpr[Double] = ScalarExpr(Cast(string.underlying, DataTypes.DoubleType))
