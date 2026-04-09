package com.netflix.wick
package column

import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.NamedTuple
import scala.Tuple.Union
import scala.compiletime.constValueTuple
import scala.annotation.implicitNotFound

@implicitNotFound("${T} is not a NamedTuple of Expr[?]")
sealed private[wick] trait Columns[T]:
  def apply(exprs: T): Seq[(name: String, expr: Expr[?])]

private[wick] object Columns:
  final class Implementation[Cols <: AnyNamedTuple](names: Tuple) extends Columns[Cols]:
    def apply(exprs: Cols): Seq[(name: String, expr: Expr[?])] =
      names.toList
        .asInstanceOf[Seq[String]]
        .zip(exprs.asInstanceOf[Tuple].toList.asInstanceOf[Seq[Expr[?]]])

  inline given [N <: Tuple, V <: Tuple] => (Union[V] <:< Expr[?]) => Columns[NamedTuple[N, V]] =
    Implementation(constValueTuple[N])
