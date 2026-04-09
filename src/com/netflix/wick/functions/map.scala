package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  def map[U](f: Expr[T] => Expr[U]): LinearExpr[Seq[U]] = LinearExpr(
    spark.sql.functions
      .transform(new Column(array.underlying), element => new Column(f(LinearExpr(element.expr)).underlying))
      .expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  def map[U](f: ScalarExpr[T] => ScalarExpr[U]): ScalarExpr[Seq[U]] = ScalarExpr(
    spark.sql.functions
      .transform(new Column(array.underlying), element => new Column(f(ScalarExpr(element.expr)).underlying))
      .expr
  )
