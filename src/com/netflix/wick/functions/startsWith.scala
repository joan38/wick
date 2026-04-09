package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  def startsWith(prefix: Expr[String]): LinearExpr[Boolean] = LinearExpr(
    spark.sql.functions.startswith(new Column(expr.underlying), new Column(prefix.underlying)).expr
  )

extension (expr: ScalarExpr[String])
  def startsWith(prefix: ScalarExpr[String]): ScalarExpr[Boolean] = ScalarExpr(
    spark.sql.functions.startswith(new Column(expr.underlying), new Column(prefix.underlying)).expr
  )
