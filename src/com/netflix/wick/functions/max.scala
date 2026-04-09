package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column
import scala.annotation.targetName

/** Aggregate function: returns the maximum value in a group.
  *
  * This function finds the maximum non-null value in the specified column within each group. When used with groupBy, it
  * finds the maximum per group. When used without groupBy, it finds the maximum of all values.
  *
  * @param col
  *   the column to find the maximum value for
  * @return
  *   a Col[T] representing the maximum value in the group
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Person(name: String, age: Int)
  * val persons = spark.createDataSeq(Seq(
  *   Person("Alice", age = 30),
  *   Person("Bob", age = 25),
  *   Person("Charlie", age = 35)
  * ))
  *
  * // Find maximum age of all persons
  * val maxAge = persons.agg(row => (oldest = max(row.age)))
  * // Result: 35
  *
  * // Find maximum age by name length group
  * val maxAgeByNameLength = persons
  *   .groupBy(row => (name_length = row.name.length))
  *   .agg(row => (max_age = max(row.age)))
  * // Result: name_length 3 -> 25, name_length 5 -> 30, name_length 7 -> 35
  *   }}}
  */
def max[T: Orderable](expr: LinearExpr[T]): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.max(new Column(expr.underlying)).expr
)

extension [T](array: Expr[Seq[T]])
  /** Returns the maximum value in an array.
    *
    * This function finds the maximum non-null value within the array. Returns null if the array is null or empty. Only
    * works with orderable types (numeric types, strings, etc.).
    *
    * @return
    *   a LinearExpr[T] representing the maximum value in the array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 78)),
    *   Student("Bob", Seq(92, 88, 95)),
    *   Student("Charlie", Seq(70, 85, 82))
    * ))
    *
    * // Find the highest grade for each student
    * val topGrades = students.select(row =>
    *   (student = row.name, best_grade = row.grades.max)
    * )
    * // Result: Alice -> 90, Bob -> 95, Charlie -> 85
    *   }}}
    */
  @targetName("arrayMax")
  def max(using Orderable[T]): LinearExpr[T | Null] = LinearExpr(
    spark.sql.functions.array_max(new Column(array.underlying)).expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Returns the maximum value in an array.
    *
    * This function finds the maximum non-null value within the array. Returns null if the array is null or empty. Only
    * works with orderable types (numeric types, strings, etc.).
    *
    * @return
    *   a LinearExpr[T] representing the maximum value in the array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 78)),
    *   Student("Bob", Seq(92, 88, 95)),
    *   Student("Charlie", Seq(70, 85, 82))
    * ))
    *
    * // Find the highest grade for each student
    * val topGrades = students.select(row =>
    *   (student = row.name, best_grade = row.grades.max)
    * )
    * // Result: Alice -> 90, Bob -> 95, Charlie -> 85
    *   }}}
    */
  @targetName("arrayMax")
  def max(using Orderable[T]): ScalarExpr[T | Null] = ScalarExpr(
    spark.sql.functions.array_max(new Column(array.underlying)).expr
  )
