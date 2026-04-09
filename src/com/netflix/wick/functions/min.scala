package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column
import scala.annotation.targetName

/** Aggregate function: returns the minimum value in a group.
  *
  * This function finds the minimum non-null value in the specified column within each group. When used with groupBy, it
  * finds the minimum per group. When used without groupBy, it finds the minimum of all values.
  *
  * @param col
  *   the column to find the minimum value for
  * @return
  *   a Col[T] representing the minimum value in the group
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
  * // Find minimum age of all persons
  * val minAge = persons.agg(row => (youngest = min(row.age)))
  * // Result: 25
  *
  * // Find minimum age by name length group
  * val minAgeByNameLength = persons
  *   .groupBy(row => (name_length = row.name.length))
  *   .agg(row => (min_age = min(row.age)))
  * // Result: name_length 3 -> 25, name_length 5 -> 30, name_length 7 -> 35
  *   }}}
  */
def min[T: Orderable](expr: LinearExpr[T]): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.min(new Column(expr.underlying)).expr
)

extension [T](array: Expr[Seq[T]])
  /** Returns the minimum value in an array.
    *
    * This function finds the minimum non-null value within the array. Returns null if the array is null or empty. Only
    * works with orderable types (numeric types, strings, etc.).
    *
    * @return
    *   a LinearExpr[T] representing the minimum value in the array
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
    * // Find the lowest grade for each student
    * val lowestGrades = students.select(row =>
    *   (student = row.name, worst_grade = row.grades.min)
    * )
    * // Result: Alice -> 78, Bob -> 88, Charlie -> 70
    *   }}}
    */
  @targetName("arrayMin")
  def min(using Orderable[T]): LinearExpr[T | Null] = LinearExpr(
    spark.sql.functions.array_min(new Column(array.underlying)).expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Returns the minimum value in an array.
    *
    * This function finds the minimum non-null value within the array. Returns null if the array is null or empty. Only
    * works with orderable types (numeric types, strings, etc.).
    *
    * @return
    *   a LinearExpr[T] representing the minimum value in the array
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
    * // Find the lowest grade for each student
    * val lowestGrades = students.select(row =>
    *   (student = row.name, worst_grade = row.grades.min)
    * )
    * // Result: Alice -> 78, Bob -> 88, Charlie -> 70
    *   }}}
    */
  @targetName("arrayMin")
  def min(using Orderable[T]): ScalarExpr[T | Null] = ScalarExpr(
    spark.sql.functions.array_min(new Column(array.underlying)).expr
  )
