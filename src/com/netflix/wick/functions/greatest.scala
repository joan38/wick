package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column

def greatest[T: Orderable](exprs: Expr[T]*): LinearExpr[T] = LinearExpr(
  spark.sql.functions.greatest(exprs.map(expr => new Column(expr.underlying))*).expr
)

def greatest[T: Orderable](exprs: ScalarExpr[T]*): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.greatest(exprs.map(expr => new Column(expr.underlying))*).expr
)
