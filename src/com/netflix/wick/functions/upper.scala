package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  /** Converts a string expression to upper case.
    *
    * @return
    *   a LinearExpr[String] containing the upper-cased value
    *
    * @example
    *   {{{
    * import com.netflix.wick.functions.upper
    *
    * persons.select(person => (upper_name = person.name.upper))
    * // "alice" -> "ALICE"
    *   }}}
    */
  def upper: LinearExpr[String] = LinearExpr(
    spark.sql.functions.upper(new Column(expr.underlying)).expr
  )

extension (expr: ScalarExpr[String])
  /** Converts a scalar string expression to upper case.
    *
    * @return
    *   a ScalarExpr[String] containing the upper-cased value
    */
  def upper: ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.upper(new Column(expr.underlying)).expr
  )
