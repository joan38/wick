package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes

extension (string: Expr[String])
  /** Casts a string expression to an Int.
    *
    * Strings that cannot be parsed as integers produce null.
    *
    * @return
    *   a LinearExpr[Int] containing the parsed integer
    */
  def toInt: LinearExpr[Int] = LinearExpr(Cast(string.underlying, DataTypes.IntegerType))

extension (string: ScalarExpr[String])
  /** Casts a scalar string expression to an Int.
    *
    * @return
    *   a ScalarExpr[Int] containing the parsed integer
    */
  def toInt: ScalarExpr[Int] = ScalarExpr(Cast(string.underlying, DataTypes.IntegerType))
