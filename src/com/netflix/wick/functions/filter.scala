package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Filters elements of an array based on a predicate.
    *
    * Returns a new array containing only the elements for which the predicate returns true.
    *
    * @param predicate
    *   a function that takes a LinearExpr[T] element and returns an Expr[Boolean]
    * @return
    *   a LinearExpr[Boolean] representing the filtered array
    */
  def filter(predicate: Expr[T] => Expr[Boolean]): LinearExpr[Seq[T]] = LinearExpr(
    spark.sql.functions
      .filter(new Column(array.underlying), column => new Column(predicate(LinearExpr(column.expr)).underlying))
      .expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Filters elements of an array based on a predicate.
    *
    * Returns a new array containing only the elements for which the predicate returns true.
    *
    * @param predicate
    *   a function that takes a LinearExpr[T] element and returns an Expr[Boolean]
    * @return
    *   a LinearExpr[Boolean] representing the filtered array
    */
  def filter(predicate: ScalarExpr[T] => ScalarExpr[Boolean]): ScalarExpr[Seq[T]] = ScalarExpr(
    spark.sql.functions
      .filter(new Column(array.underlying), column => new Column(predicate(ScalarExpr(column.expr)).underlying))
      .expr
  )
