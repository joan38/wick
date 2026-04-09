package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Sorts the array in ascending order by default.
    *
    * This function sorts the elements in the array. By default, it sorts in ascending order. Only works with orderable
    * types (numeric types, strings, etc.).
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the sorted array in ascending order
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 78, 92))
    * ))
    *
    * // Sort grades in ascending order (default)
    * val sortedDesc = students.select(row => (sorted_grades = row.grades.sorted))
    * // Result: [92, 90, 85, 78]
    *   }}}
    */
  def sorted(using Orderable[T]): LinearExpr[Seq[T]] = sorted(asc = true)

  /** Sorts the array in the specified order.
    *
    * This function sorts the elements in the array in ascending or descending order as specified. Only works with
    * orderable types (numeric types, strings, etc.).
    *
    * @param asc
    *   true for ascending order, false for descending order
    * @return
    *   a LinearExpr[Seq[T]] representing the sorted array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 78, 92)),
    *   Student("Bob", Seq(95, 82, 88))
    * ))
    *
    * // Sort grades in ascending order
    * val sortedAsc = students.select(row => (sorted_asc = row.grades.sorted(asc = true)))
    * // Result: Alice -> [78, 85, 90, 92], Bob -> [82, 88, 95]
    *
    * // Sort grades in descending order
    * val sortedDesc = students.select(row => (sorted_desc = row.grades.sorted(asc = false)))
    * // Result: Alice -> [92, 90, 85, 78], Bob -> [95, 88, 82]
    *   }}}
    */
  def sorted(asc: Boolean)(using Orderable[T]): LinearExpr[Seq[T]] = LinearExpr(
    spark.sql.functions.sort_array(new Column(array.underlying), asc).expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Sorts the array in ascending order by default.
    *
    * This function sorts the elements in the array. By default, it sorts in ascending order. Only works with orderable
    * types (numeric types, strings, etc.).
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the sorted array in ascending order
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 78, 92))
    * ))
    *
    * // Sort grades in ascending order (default)
    * val sortedDesc = students.select(row => (sorted_grades = row.grades.sorted))
    * // Result: [92, 90, 85, 78]
    *   }}}
    */
  def sorted(using Orderable[T]): ScalarExpr[Seq[T]] = sorted(asc = true)

  /** Sorts the array in the specified order.
    *
    * This function sorts the elements in the array in ascending or descending order as specified. Only works with
    * orderable types (numeric types, strings, etc.).
    *
    * @param asc
    *   true for ascending order, false for descending order
    * @return
    *   a LinearExpr[Seq[T]] representing the sorted array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Student(name: String, grades: Seq[Int])
    * val students = spark.createDataSeq(Seq(
    *   Student("Alice", Seq(85, 90, 78, 92)),
    *   Student("Bob", Seq(95, 82, 88))
    * ))
    *
    * // Sort grades in ascending order
    * val sortedAsc = students.select(row => (sorted_asc = row.grades.sorted(asc = true)))
    * // Result: Alice -> [78, 85, 90, 92], Bob -> [82, 88, 95]
    *
    * // Sort grades in descending order
    * val sortedDesc = students.select(row => (sorted_desc = row.grades.sorted(asc = false)))
    * // Result: Alice -> [92, 90, 85, 78], Bob -> [95, 88, 82]
    *   }}}
    */
  def sorted(asc: Boolean)(using Orderable[T]): ScalarExpr[Seq[T]] = ScalarExpr(
    spark.sql.functions.sort_array(new Column(array.underlying), asc).expr
  )
