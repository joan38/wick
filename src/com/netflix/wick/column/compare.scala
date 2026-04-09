package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual

extension [A, ExpA <: Expr](a: ExpA[A])
  infix def <[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[Boolean] =
    exprCreator(LessThan(a.underlying, b.underlying))

  infix def <[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[Boolean] =
    exprCreator(LessThan(a.underlying, lit(b).underlying))

  infix def >[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[Boolean] =
    exprCreator(GreaterThan(a.underlying, b.underlying))

  infix def >[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[Boolean] =
    exprCreator(GreaterThan(a.underlying, lit(b).underlying))

  infix def <=[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[Boolean] =
    exprCreator(LessThanOrEqual(a.underlying, b.underlying))

  infix def <=[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[Boolean] =
    exprCreator(LessThanOrEqual(a.underlying, lit(b).underlying))

  infix def >=[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[Boolean] =
    exprCreator(GreaterThanOrEqual(a.underlying, b.underlying))

  infix def >=[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[Boolean] =
    exprCreator(GreaterThanOrEqual(a.underlying, lit(b).underlying))
