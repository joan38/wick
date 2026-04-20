package com.netflix.wick
package functions

import com.netflix.wick.column.NullLabel

extension [T](array: Expr[Seq[T]])
  /** Applies a nullable-producing function to each element, then drops the resulting nulls.
    *
    * Unlike a plain [[map]], the function body runs in a `NullLabel` context so accessing nullable columns within does
    * not pollute the result type: nulls produced by the function are filtered out, yielding `Seq[U]` rather than
    * `Seq[U | Null]`.
    *
    * @param f
    *   a function producing a value in a `NullLabel` context; returning null filters out the element
    * @tparam U
    *   the element type of the resulting array
    * @return
    *   a LinearExpr[Seq[U]] with null results removed
    */
  def flatMapNulls[U](f: Expr[T] => NullLabel ?=> Expr[U]): LinearExpr[Seq[U]] =
    array.map(element => nullable(f(element))).filter(_.isNotNull).asInstanceOf[LinearExpr[Seq[U]]]

extension [T](array: ScalarExpr[Seq[T]])
  /** Applies a nullable-producing function to each element of a scalar array, then drops the resulting nulls.
    *
    * @param f
    *   a function producing a value in a `NullLabel` context; returning null filters out the element
    * @tparam U
    *   the element type of the resulting array
    * @return
    *   a ScalarExpr[Seq[U]] with null results removed
    */
  def flatMapNulls[U](f: ScalarExpr[T] => NullLabel ?=> ScalarExpr[U]): ScalarExpr[Seq[U]] =
    array.map(element => nullable(f(element))).filter(_.isNotNull).asInstanceOf[ScalarExpr[Seq[U]]]
