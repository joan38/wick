package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (string: Expr[String])
  /** Strips leading and trailing whitespace from the string.
    *
    * @return
    *   a LinearExpr[String] with whitespace removed from both ends
    */
  def trim: LinearExpr[String] = LinearExpr(
    spark.sql.functions.trim(new Column(string.underlying)).expr
  )

  /** Strips leading and trailing occurrences of each character in `trimString` from the string.
    *
    * @param trimString
    *   the set of characters to strip from both ends
    * @return
    *   a LinearExpr[String] with the specified characters removed from both ends
    */
  def trim(trimString: String): LinearExpr[String] = LinearExpr(
    spark.sql.functions.trim(new Column(string.underlying), trimString).expr
  )

extension (string: ScalarExpr[String])
  /** Strips leading and trailing whitespace from a scalar string.
    *
    * @return
    *   a ScalarExpr[String] with whitespace removed from both ends
    */
  def trim: ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.trim(new Column(string.underlying)).expr
  )

  /** Strips leading and trailing occurrences of each character in `trimString` from a scalar string.
    *
    * @param trimString
    *   the set of characters to strip from both ends
    * @return
    *   a ScalarExpr[String] with the specified characters removed from both ends
    */
  def trim(trimString: String): ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.trim(new Column(string.underlying), trimString).expr
  )
