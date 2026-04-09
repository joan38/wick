package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (string: Expr[String])
  def trim: LinearExpr[String] = LinearExpr(
    spark.sql.functions.trim(new Column(string.underlying)).expr
  )

  def trim(trimString: String): LinearExpr[String] = LinearExpr(
    spark.sql.functions.trim(new Column(string.underlying), trimString).expr
  )

extension (string: ScalarExpr[String])
  def trim: ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.trim(new Column(string.underlying)).expr
  )

  def trim(trimString: String): ScalarExpr[String] = ScalarExpr(
    spark.sql.functions.trim(new Column(string.underlying), trimString).expr
  )
