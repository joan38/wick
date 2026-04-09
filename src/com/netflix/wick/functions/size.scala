package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Returns the size (length) of an array column.
    *
    * This function calculates the number of elements in an array. Returns null if the input array is null.
    *
    * @param col
    *   the array column to get the size of
    * @return
    *   a LinearExpr[Int] representing the array size
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 88)),
    *   Student("Bob", Seq(78, 82))
    * ))
    *
    * // Get the number of grades for each student
    * val gradeCount = students.select(row => (size = row.grades.size))
    * // Result: 3, 2
    *   }}}
    */
  def size: LinearExpr[Int] = LinearExpr(spark.sql.functions.size(new Column(array.underlying)).expr)

extension [T](array: ScalarExpr[Seq[T]])
  /** Returns the size (length) of an array column.
    *
    * This function calculates the number of elements in an array. Returns null if the input array is null.
    *
    * @param col
    *   the array column to get the size of
    * @return
    *   a LinearExpr[Int] representing the array size
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 88)),
    *   Student("Bob", Seq(78, 82))
    * ))
    *
    * // Get the number of grades for each student
    * val gradeCount = students.select(row => (size = row.grades.size))
    * // Result: 3, 2
    *   }}}
    */
  def size: ScalarExpr[Int] = ScalarExpr(spark.sql.functions.size(new Column(array.underlying)).expr)
