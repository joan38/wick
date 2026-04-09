package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column

def least[T: Orderable](exprs: Expr[T]*): LinearExpr[T] = LinearExpr(
  spark.sql.functions.least(exprs.map(expr => new Column(expr.underlying))*).expr
)

def least[T: Orderable](exprs: ScalarExpr[T]*): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.least(exprs.map(expr => new Column(expr.underlying))*).expr
)
