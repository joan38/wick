package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Reduces the elements of an array into a single value using an initial value and a merge function.
    *
    * The merge function is applied left-to-right, combining the running accumulator with each element.
    *
    * @param initialValue
    *   the initial value of the accumulator
    * @param merge
    *   a function that combines the accumulator with the next element
    * @tparam R
    *   the type of the accumulator and final result
    * @return
    *   a LinearExpr containing the aggregated value
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    * import com.netflix.wick.column.lit
    *
    * case class GradeBook(student: String, grades: Seq[Int])
    * val gradeBooks = spark.createDataSeq(Seq(GradeBook("Alice", Seq(85, 90, 78))))
    *
    * gradeBooks.select(row => (total = row.grades.aggregate(lit(0), _ + _)))
    * // Result: 253
    *   }}}
    */
  def aggregate[R](initialValue: Expr[R], merge: (LinearExpr[R], LinearExpr[T]) => LinearExpr[R]): LinearExpr[R] =
    LinearExpr(
      spark.sql.functions
        .aggregate(
          new Column(array.underlying),
          new Column(initialValue.underlying),
          (acc, e) => new Column(merge(LinearExpr(acc.expr), LinearExpr(e.expr)).underlying)
        )
        .expr
    )

extension [T](array: ScalarExpr[Seq[T]])
  /** Reduces the elements of a scalar array into a single value using an initial value and a merge function.
    *
    * The merge function is applied left-to-right, combining the running accumulator with each element.
    *
    * @param initialValue
    *   the initial value of the accumulator
    * @param merge
    *   a function that combines the accumulator with the next element
    * @tparam R
    *   the type of the accumulator and final result
    * @return
    *   a ScalarExpr containing the aggregated value
    */
  def aggregate[R](initialValue: ScalarExpr[R], merge: (ScalarExpr[R], ScalarExpr[T]) => ScalarExpr[R]): ScalarExpr[R] =
    ScalarExpr(
      spark.sql.functions
        .aggregate(
          new Column(array.underlying),
          new Column(initialValue.underlying),
          (acc, e) => new Column(merge(ScalarExpr(acc.expr), ScalarExpr(e.expr)).underlying)
        )
        .expr
    )
