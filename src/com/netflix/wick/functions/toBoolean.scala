package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.DataTypes

extension (string: Expr[String])
  /** Casts a string expression to a Boolean.
    *
    * Follows Spark's string-to-boolean casting rules: values like `"true"` / `"false"` (case-insensitive) are parsed;
    * invalid strings produce null.
    *
    * @return
    *   a LinearExpr[Boolean] containing the parsed boolean
    */
  def toBoolean: LinearExpr[Boolean] = LinearExpr(Cast(string.underlying, DataTypes.BooleanType))

extension (string: ScalarExpr[String])
  /** Casts a scalar string expression to a Boolean.
    *
    * @return
    *   a ScalarExpr[Boolean] containing the parsed boolean
    */
  def toBoolean: ScalarExpr[Boolean] = ScalarExpr(Cast(string.underlying, DataTypes.BooleanType))
