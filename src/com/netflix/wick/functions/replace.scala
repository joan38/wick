package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  /** Replaces all occurrences of a literal substring with another string.
    *
    * @param search
    *   the literal substring to search for
    * @param replacement
    *   the string to replace each occurrence with
    * @return
    *   a LinearExpr[String] with all occurrences replaced
    */
  def replace(search: Expr[String], replacement: Expr[String]): LinearExpr[String] = LinearExpr(
    spark.sql.functions
      .replace(new Column(expr.underlying), new Column(search.underlying), new Column(replacement.underlying))
      .expr
  )

extension (expr: ScalarExpr[String])
  /** Replaces all occurrences of a literal substring with another string in a scalar expression.
    *
    * @param search
    *   the literal substring to search for
    * @param replacement
    *   the string to replace each occurrence with
    * @return
    *   a ScalarExpr[String] with all occurrences replaced
    */
  def replace(search: ScalarExpr[String], replacement: ScalarExpr[String]): ScalarExpr[String] = ScalarExpr(
    spark.sql.functions
      .replace(new Column(expr.underlying), new Column(search.underlying), new Column(replacement.underlying))
      .expr
  )

extension (expr: Expr[String])
  /** Replaces all substrings matching a regular expression pattern with a replacement string.
    *
    * @param pattern
    *   the regular expression pattern to match
    * @param replacement
    *   the replacement string (may contain back-references like `$1`)
    * @return
    *   a LinearExpr[String] with all matches replaced
    */
  def replaceRegex(pattern: Expr[String], replacement: Expr[String]): LinearExpr[String] = LinearExpr(
    spark.sql.functions
      .regexp_replace(new Column(expr.underlying), new Column(pattern.underlying), new Column(replacement.underlying))
      .expr
  )

extension (expr: ScalarExpr[String])
  /** Replaces all substrings matching a regular expression pattern in a scalar expression.
    *
    * @param pattern
    *   the regular expression pattern to match
    * @param replacement
    *   the replacement string (may contain back-references like `$1`)
    * @return
    *   a ScalarExpr[String] with all matches replaced
    */
  def replaceRegex(pattern: ScalarExpr[String], replacement: ScalarExpr[String]): ScalarExpr[String] = ScalarExpr(
    spark.sql.functions
      .regexp_replace(new Column(expr.underlying), new Column(pattern.underlying), new Column(replacement.underlying))
      .expr
  )
