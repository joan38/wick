package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Checks if any element in the array satisfies the given predicate.
    *
    * This function returns true if at least one element in the array matches the condition, false if no elements match.
    * Returns null if the input array is null.
    *
    * @param col
    *   the array column to check
    * @param predicate
    *   the condition to test each element against
    * @return
    *   a LinearExpr[Boolean] indicating whether any element satisfies the predicate
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 88)),
    *   Student("Bob", Seq(78, 82, 75))
    * ))
    *
    * // Check if any grade is above 90
    * val hasHighGrade = students.select(row =>
    *   (is_good = row.grades.exists(grade => grade > 90))
    * )
    * // Result: true, false
    *   }}}
    */
  def exists(predicate: Expr[T] => Expr[Boolean]): LinearExpr[Boolean] = LinearExpr(
    spark.sql.functions
      .exists(new Column(array.underlying), column => new Column(predicate(LinearExpr(column.expr)).underlying))
      .expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Checks if any element in the array satisfies the given predicate.
    *
    * This function returns true if at least one element in the array matches the condition, false if no elements match.
    * Returns null if the input array is null.
    *
    * @param col
    *   the array column to check
    * @param predicate
    *   the condition to test each element against
    * @return
    *   a LinearExpr[Boolean] indicating whether any element satisfies the predicate
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 88)),
    *   Student("Bob", Seq(78, 82, 75))
    * ))
    *
    * // Check if any grade is above 90
    * val hasHighGrade = students.select(row =>
    *   (is_good = row.grades.exists(grade => grade > 90))
    * )
    * // Result: true, false
    *   }}}
    */
  def exists(predicate: ScalarExpr[T] => ScalarExpr[Boolean]): ScalarExpr[Boolean] = ScalarExpr(
    spark.sql.functions
      .exists(new Column(array.underlying), column => new Column(predicate(ScalarExpr(column.expr)).underlying))
      .expr
  )
