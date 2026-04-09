package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  def explode: LinearExpr[T] = LinearExpr(
    spark.sql.functions.explode(new Column(array.underlying)).expr
  )
