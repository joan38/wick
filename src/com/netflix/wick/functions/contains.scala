package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Checks if an array contains a specific value.
    *
    * This function returns true if the array contains the specified value, false otherwise. Returns null if the input
    * array is null.
    *
    * @param col
    *   the array column to search in
    * @param value
    *   the value to search for
    * @return
    *   a LinearExpr[Boolean] indicating whether the value is found
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 88)),
    *   Student("Bob", Seq(78, 82, 85))
    * ))
    *
    * // Check if any student has a grade of 85
    * val hasGrade85 = students.select(row => (is_85 = row.grades.contains(85)))
    * // Result: true, true
    *   }}}
    */
  def contains(value: T): LinearExpr[Boolean] = LinearExpr(
    spark.sql.functions.array_contains(new Column(array.underlying), value).expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Checks if an array contains a specific value.
    *
    * This function returns true if the array contains the specified value, false otherwise. Returns null if the input
    * array is null.
    *
    * @param col
    *   the array column to search in
    * @param value
    *   the value to search for
    * @return
    *   a LinearExpr[Boolean] indicating whether the value is found
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 88)),
    *   Student("Bob", Seq(78, 82, 85))
    * ))
    *
    * // Check if any student has a grade of 85
    * val hasGrade85 = students.select(row => (is_85 = row.grades.contains(85)))
    * // Result: true, true
    *   }}}
    */
  def contains(value: T): ScalarExpr[Boolean] = ScalarExpr(
    spark.sql.functions.array_contains(new Column(array.underlying), value).expr
  )
