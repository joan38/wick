package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.expressions.Expression
import scala.NamedTuple.AnyNamedTuple
import scala.util.NotGiven

sealed private[wick] trait ExprCreator[In]:
  type Exp <: Expr
  def apply[Out](expr: Expression): Exp[Out]

private[wick] object ExprCreator:
  given [I] => ExprCreator[Expr[I]]:
    type Exp = LinearExpr
    def apply[Out](expr: Expression) = LinearExpr(expr)

  given [I] => ExprCreator[LinearExpr[I]]:
    type Exp = LinearExpr
    def apply[Out](expr: Expression) = LinearExpr(expr)

  given [I] => ExprCreator[ScalarExpr[I]]:
    type Exp = ScalarExpr
    def apply[Out](expr: Expression) = ScalarExpr(expr)

  given scalarCols: [Cols <: AnyNamedTuple] => (IsScalarCols[Cols]) => ExprCreator[Cols]:
    type Exp = ScalarExpr
    def apply[Out](expr: Expression) = ScalarExpr(expr)

  given linearCols: [Cols <: AnyNamedTuple] => (NotGiven[IsScalarCols[Cols]]) => ExprCreator[Cols]:
    type Exp = LinearExpr
    def apply[Out](expr: Expression) = LinearExpr(expr)

  given scalarExprs: [Exprs <: Tuple] => (IsScalarExprs[Exprs]) => ExprCreator[Exprs]:
    type Exp = ScalarExpr
    def apply[Out](expr: Expression) = ScalarExpr(expr)

  given linearExprs: [Exprs <: Tuple] => (NotGiven[IsScalarExprs[Exprs]]) => ExprCreator[Exprs]:
    type Exp = LinearExpr
    def apply[Out](expr: Expression) = LinearExpr(expr)
