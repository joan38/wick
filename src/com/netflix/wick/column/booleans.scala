package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.expressions.*

extension [A <: Boolean | Null](boolean: Expr[A])
  def unary_! : LinearExpr[Not[A]] = LinearExpr(Not(boolean.underlying))

  infix def &&[B <: Boolean | Null](other: Expr[B]): LinearExpr[Logical[A, B]] =
    LinearExpr(And(boolean.underlying, other.underlying))

  infix def ||[B <: Boolean | Null](other: Expr[B]): LinearExpr[Logical[A, B]] =
    LinearExpr(Or(boolean.underlying, other.underlying))

extension [A <: Boolean | Null](boolean: ScalarExpr[A])
  def unary_! : ScalarExpr[Not[A]] = ScalarExpr(Not(boolean.underlying))

  infix def &&[B <: Boolean | Null](other: ScalarExpr[B]): ScalarExpr[Logical[A, B]] =
    ScalarExpr(And(boolean.underlying, other.underlying))

  infix def ||[B <: Boolean | Null](other: ScalarExpr[B]): ScalarExpr[Logical[A, B]] =
    ScalarExpr(Or(boolean.underlying, other.underlying))

type Not[A] = HasNull[A] match
  case true  => Boolean | Null
  case false => Boolean

type Logical[A, B] = (HasNull[A], HasNull[B]) match
  case (false, false) => Boolean
  case _              => Boolean | Null
