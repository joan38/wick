package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.expressions.*
import scala.annotation.implicitNotFound

extension [A, ExpA <: Expr](a: ExpA[A])
  infix def ===[B, ExpB <: Expr](
      b: ExpB[B]
  )(using eq: Eq[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[Boolean] =
    exprCreator(EqualNullSafe(a.underlying, b.underlying))

  infix def ===[B](
      b: B
  )(using eq: Eq[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[Boolean] =
    exprCreator(EqualNullSafe(a.underlying, lit(b).underlying))

  infix def !==[B, ExpB <: Expr](
      b: ExpB[B]
  )(using eq: Eq[A, B], exprCreator: ExprCreator[(ExpA[A], ExpB[B])]): exprCreator.Exp[Boolean] =
    exprCreator(Not(EqualNullSafe(a.underlying, b.underlying)))

  infix def !==[B](
      b: B
  )(using eq: Eq[A, B], exprCreator: ExprCreator[ExpA[A] *: EmptyTuple]): exprCreator.Exp[Boolean] =
    exprCreator(Not(EqualNullSafe(a.underlying, lit(b).underlying)))

@implicitNotFound("Cannot compare types ${A} and ${B}. No implicit Eq[${A}, ${B}] found")
trait Eq[A, B]

object Eq:
  given [A] => Eq[A, A]                      = new Eq {}
  given Eq[Long, Int]                        = new Eq {}
  given Eq[Int, Long]                        = new Eq {}
  given [A] => (Eq[A, A]) => Eq[A | Null, A] = new Eq {}
  given [A] => (Eq[A, A]) => Eq[A, A | Null] = new Eq {}
