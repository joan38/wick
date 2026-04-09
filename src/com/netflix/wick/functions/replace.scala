package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  def replace(search: Expr[String], replacement: Expr[String]): LinearExpr[String] = LinearExpr(
    spark.sql.functions
      .replace(new Column(expr.underlying), new Column(search.underlying), new Column(replacement.underlying))
      .expr
  )

extension (expr: ScalarExpr[String])
  def replace(search: ScalarExpr[String], replacement: ScalarExpr[String]): ScalarExpr[String] = ScalarExpr(
    spark.sql.functions
      .replace(new Column(expr.underlying), new Column(search.underlying), new Column(replacement.underlying))
      .expr
  )

extension (expr: Expr[String])
  def replaceRegex(pattern: Expr[String], replacement: Expr[String]): LinearExpr[String] = LinearExpr(
    spark.sql.functions
      .regexp_replace(new Column(expr.underlying), new Column(pattern.underlying), new Column(replacement.underlying))
      .expr
  )

extension (expr: ScalarExpr[String])
  def replaceRegex(pattern: ScalarExpr[String], replacement: ScalarExpr[String]): ScalarExpr[String] = ScalarExpr(
    spark.sql.functions
      .regexp_replace(new Column(expr.underlying), new Column(pattern.underlying), new Column(replacement.underlying))
      .expr
  )
