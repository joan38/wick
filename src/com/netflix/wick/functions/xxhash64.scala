package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](expr: Expr[T])
  def xxhash64: LinearExpr[Long] = LinearExpr(
    spark.sql.functions.xxhash64(new Column(expr.underlying)).expr
  )

extension [T](expr: ScalarExpr[T])
  def xxhash64: ScalarExpr[Long] = ScalarExpr(
    spark.sql.functions.xxhash64(new Column(expr.underlying)).expr
  )
