package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  def lower: LinearExpr[String] = LinearExpr(
    spark.sql.functions.lower(new Column(expr.underlying)).expr
  )

extension (expr: ScalarExpr[String])
  def lower: ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.lower(new Column(expr.underlying)).expr
  )
