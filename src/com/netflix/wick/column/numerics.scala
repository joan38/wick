package com.netflix.wick.column

import org.apache.spark.sql.catalyst.expressions.{Add, Divide, Multiply, Remainder, Subtract, UnaryMinus}

extension [A, ExpA <: Expr](a: ExpA[A])
  def unary_-(using op: NumericOp[A, A], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[op.Result] =
    exprCreator(UnaryMinus(a.underlying))

  infix def +[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[op.Result] =
    exprCreator(Add(a.underlying, b.underlying))

  infix def +[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[op.Result] =
    exprCreator(Add(a.underlying, lit(b).underlying))

  infix def -[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[op.Result] =
    exprCreator(Subtract(a.underlying, b.underlying))

  infix def -[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[op.Result] =
    exprCreator(Subtract(a.underlying, lit(b).underlying))

  infix def *[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[op.Result] =
    exprCreator(Multiply(a.underlying, b.underlying))

  infix def *[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[op.Result] =
    exprCreator(Multiply(a.underlying, lit(b).underlying))

  infix def /[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[op.Result] =
    exprCreator(Divide(a.underlying, b.underlying))

  infix def /[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[op.Result] =
    exprCreator(Divide(a.underlying, lit(b).underlying))

  infix def %[B, ExpB <: Expr](
      b: ExpB[B]
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[op.Result] =
    exprCreator(Remainder(a.underlying, b.underlying))

  infix def %[B](
      b: B
  )(using op: NumericOp[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[op.Result] =
    exprCreator(Remainder(a.underlying, lit(b).underlying))

trait NumericOp[A, B]:
  type Result

object NumericOp:
  given NumericOp[Int, Int]:
    type Result = Int
  given NumericOp[Int, Long]:
    type Result = Long
  given NumericOp[Int, Double]:
    type Result = Double
  given NumericOp[Int, Float]:
    type Result = Float

  given NumericOp[Long, Long]:
    type Result = Long
  given NumericOp[Long, Int]:
    type Result = Long
  given NumericOp[Long, Double]:
    type Result = Double
  given NumericOp[Long, Float]:
    type Result = Float

  given NumericOp[Float, Float]:
    type Result = Float
  given NumericOp[Float, Int]:
    type Result = Float
  given NumericOp[Float, Long]:
    type Result = Float
  given NumericOp[Float, Double]:
    type Result = Double

  given NumericOp[Double, Double]:
    type Result = Double
  given NumericOp[Double, Int]:
    type Result = Double
  given NumericOp[Double, Long]:
    type Result = Double
  given NumericOp[Double, Float]:
    type Result = Double

  given NumericOp[java.math.BigDecimal, java.math.BigDecimal]:
    type Result = java.math.BigDecimal
