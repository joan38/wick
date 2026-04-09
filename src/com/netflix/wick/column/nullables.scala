package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.types.NullType

extension [T](expr: Expr[T | Null])
  def ?(using label: NullLabel): LinearExpr[T] =
    label.addNullableExpr(expr.underlying)
    LinearExpr(expr.underlying)

  def isNull: LinearExpr[Boolean]    = LinearExpr(IsNull(expr.underlying))
  def isNotNull: LinearExpr[Boolean] = LinearExpr(IsNotNull(expr.underlying))

  def orElse[U >: T](fallback: Expr[U] | U): LinearExpr[U] = fallback match
    case fallback: Expr[U] @unchecked => LinearExpr(Coalesce(Seq(expr.underlying, fallback.underlying)))
    case fallback: U @unchecked       => LinearExpr(Coalesce(Seq(expr.underlying, lit(fallback).underlying)))

extension [T](expr: ScalarExpr[T | Null])
  def ?(using label: NullLabel): ScalarExpr[T] =
    label.addNullableExpr(expr.underlying)
    ScalarExpr(expr.underlying)

  def isNull: ScalarExpr[Boolean]    = ScalarExpr(IsNull(expr.underlying))
  def isNotNull: ScalarExpr[Boolean] = ScalarExpr(IsNotNull(expr.underlying))

  def orElse[U >: T](fallback: ScalarExpr[U] | U): ScalarExpr[U] = fallback match
    case fallback: Expr[U] @unchecked => ScalarExpr(Coalesce(Seq(expr.underlying, fallback.underlying)))
    case fallback: U @unchecked       => ScalarExpr(Coalesce(Seq(expr.underlying, lit(fallback).underlying)))

extension [T](ref: DataSeq.Ref[T | Null]) def ? : DataSeq.Ref[T] = DataSeq.Ref(ref.dataFrame)

def nullable[T, Exp <: Expr](body: NullLabel ?=> Exp[T])(using
    exprCreator: ExprCreator[Exp[T]]
): exprCreator.Exp[T | Null] =
  val label = NullLabel()
  val expr  = body(using label)
  label.noNullsCheck match
    case Some(check) => exprCreator(If(check, expr.underlying, Literal.create(null, NullType)))
    case None        => exprCreator(expr.underlying)

class NullLabel:
  private var nullableExprs                   = Seq.empty[Expression]
  def addNullableExpr(expr: Expression): Unit = nullableExprs = nullableExprs :+ expr
  def noNullsCheck: Option[Expression]        = nullableExprs.map(IsNotNull(_)).reduceOption(And(_, _))
