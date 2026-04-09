package com.netflix.wick
package functions

import com.netflix.wick.column.NullLabel

extension [T](array: Expr[Seq[T]])
  def flatMapNulls[U](f: Expr[T] => NullLabel ?=> Expr[U]): LinearExpr[Seq[U]] =
    array.map(element => nullable(f(element))).filter(_.isNotNull).asInstanceOf[LinearExpr[Seq[U]]]

extension [T](array: ScalarExpr[Seq[T]])
  def flatMapNulls[U](f: ScalarExpr[T] => NullLabel ?=> ScalarExpr[U]): ScalarExpr[Seq[U]] =
    array.map(element => nullable(f(element))).filter(_.isNotNull).asInstanceOf[ScalarExpr[Seq[U]]]
