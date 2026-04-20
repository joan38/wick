package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes
import scala.annotation.targetName
import com.netflix.wick.column.Numeric

extension (string: Expr[String])
  /** Casts a string expression to a Long.
    *
    * Strings that cannot be parsed as long integers produce null.
    *
    * @return
    *   a LinearExpr[Long] containing the parsed long
    */
  def toLong: LinearExpr[Long] = LinearExpr(Cast(string.underlying, DataTypes.LongType))

extension (string: ScalarExpr[String])
  /** Casts a scalar string expression to a Long.
    *
    * @return
    *   a ScalarExpr[Long] containing the parsed long
    */
  def toLong: ScalarExpr[Long] = ScalarExpr(Cast(string.underlying, DataTypes.LongType))

extension (numeric: Expr[Numeric])
  /** Casts a numeric expression to a Long, widening or narrowing as needed.
    *
    * @return
    *   a LinearExpr[Long] containing the converted value
    */
  @targetName("numericToLong")
  def toLong: LinearExpr[Long] = LinearExpr(Cast(numeric.underlying, DataTypes.LongType))

extension (numeric: ScalarExpr[Numeric])
  /** Casts a numeric scalar expression to a Long, widening or narrowing as needed.
    *
    * @return
    *   a ScalarExpr[Long] containing the converted value
    */
  @targetName("numericToLong")
  def toLong: ScalarExpr[Long] = ScalarExpr(Cast(numeric.underlying, DataTypes.LongType))
