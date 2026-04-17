package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  def aggregate[R](initialValue: Expr[R], merge: (LinearExpr[R], LinearExpr[T]) => LinearExpr[R]): LinearExpr[R] =
    LinearExpr(
      spark.sql.functions
        .aggregate(
          new Column(array.underlying),
          new Column(initialValue.underlying),
          (acc, e) => new Column(merge(LinearExpr(acc.expr), LinearExpr(e.expr)).underlying)
        )
        .expr
    )

extension [T](array: ScalarExpr[Seq[T]])
  def aggregate[R](initialValue: ScalarExpr[R], merge: (ScalarExpr[R], ScalarExpr[T]) => ScalarExpr[R]): ScalarExpr[R] =
    ScalarExpr(
      spark.sql.functions
        .aggregate(
          new Column(array.underlying),
          new Column(initialValue.underlying),
          (acc, e) => new Column(merge(ScalarExpr(acc.expr), ScalarExpr(e.expr)).underlying)
        )
        .expr
    )
