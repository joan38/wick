package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  /** Converts a string expression to lower case.
    *
    * @return
    *   a LinearExpr[String] containing the lower-cased value
    *
    * @example
    *   {{{
    * import com.netflix.wick.functions.lower
    *
    * persons.select(person => (lower_name = person.name.lower))
    * // "ALICE" -> "alice"
    *   }}}
    */
  def lower: LinearExpr[String] = LinearExpr(
    spark.sql.functions.lower(new Column(expr.underlying)).expr
  )

extension (expr: ScalarExpr[String])
  /** Converts a scalar string expression to lower case.
    *
    * @return
    *   a ScalarExpr[String] containing the lower-cased value
    */
  def lower: ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.lower(new Column(expr.underlying)).expr
  )
