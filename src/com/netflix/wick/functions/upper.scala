package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  def upper: LinearExpr[String] = LinearExpr(
    spark.sql.functions.upper(new Column(expr.underlying)).expr
  )

extension (expr: ScalarExpr[String])
  def upper: ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.upper(new Column(expr.underlying)).expr
  )
