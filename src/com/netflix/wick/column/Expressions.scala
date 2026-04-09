package com.netflix.wick
package column

import scala.Tuple.Union
import scala.annotation.implicitNotFound

@implicitNotFound("${T} should be either a single Expr[?] or a tuple of Expr[?]")
sealed private[wick] trait Expressions[T]:
  def apply(o: T): Seq[Expr[?]]

private[wick] object Expressions:
  given [Cols <: Tuple] => (Union[Cols] <:< Expr[?]) => Expressions[Cols]:
    def apply(tuple: Cols): Seq[Expr[?]] = tuple.toList.asInstanceOf[Seq[Expr[?]]]

  given [T, Expression <: Expr[T]] => Expressions[Expression]:
    def apply(expr: Expression): Seq[Expression] = Seq(expr)
