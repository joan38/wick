package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Applies a transformation to each element of the array.
    *
    * @param f
    *   a function mapping each element to a new value
    * @tparam U
    *   the element type of the resulting array
    * @return
    *   a LinearExpr[Seq[U]] containing the transformed elements
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    * import com.netflix.wick.column.lit
    *
    * case class GradeBook(student: String, grades: Seq[Int])
    * val gradeBooks = spark.createDataSeq(Seq(GradeBook("Alice", Seq(85, 90, 78))))
    *
    * gradeBooks.select(row => (curved = row.grades.map(_ + lit(5))))
    * // Result: [90, 95, 83]
    *   }}}
    */
  def map[U](f: Expr[T] => Expr[U]): LinearExpr[Seq[U]] = LinearExpr(
    spark.sql.functions
      .transform(new Column(array.underlying), element => new Column(f(LinearExpr(element.expr)).underlying))
      .expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Applies a transformation to each element of a scalar array.
    *
    * @param f
    *   a function mapping each element to a new value
    * @tparam U
    *   the element type of the resulting array
    * @return
    *   a ScalarExpr[Seq[U]] containing the transformed elements
    */
  def map[U](f: ScalarExpr[T] => ScalarExpr[U]): ScalarExpr[Seq[U]] = ScalarExpr(
    spark.sql.functions
      .transform(new Column(array.underlying), element => new Column(f(ScalarExpr(element.expr)).underlying))
      .expr
  )
