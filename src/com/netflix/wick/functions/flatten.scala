package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[Seq[T]]])
  /** Flattens a nested array into a single-level array.
    *
    * This function takes an array of arrays and returns a single array containing all the elements from the nested
    * arrays. The order of elements is preserved. Returns null if the input array is null.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the flattened array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Course(name: String, studentGroups: Seq[Seq[String]])
    * val courses = spark.createDataSeq(Seq(
    *   Course("Math", Seq(Seq("Alice", "Bob"), Seq("Charlie"), Seq("David", "Eve"))),
    *   Course("Science", Seq(Seq("Frank", "Grace"), Seq("Henry")))
    * ))
    *
    * // Flatten nested student groups into a single list
    * val allStudents = courses.select(row =>
    *   (course = row.name, students = row.studentGroups.flatten)
    * )
    * // Result: Math -> ["Alice", "Bob", "Charlie", "David", "Eve"], Science -> ["Frank", "Grace", "Henry"]
    *   }}}
    */
  def flatten: LinearExpr[Seq[T]] = LinearExpr(spark.sql.functions.flatten(new Column(array.underlying)).expr)

extension [T](array: ScalarExpr[Seq[Seq[T]]])
  /** Flattens a nested array into a single-level array.
    *
    * This function takes an array of arrays and returns a single array containing all the elements from the nested
    * arrays. The order of elements is preserved. Returns null if the input array is null.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the flattened array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Course(name: String, studentGroups: Seq[Seq[String]])
    * val courses = spark.createDataSeq(Seq(
    *   Course("Math", Seq(Seq("Alice", "Bob"), Seq("Charlie"), Seq("David", "Eve"))),
    *   Course("Science", Seq(Seq("Frank", "Grace"), Seq("Henry")))
    * ))
    *
    * // Flatten nested student groups into a single list
    * val allStudents = courses.select(row =>
    *   (course = row.name, students = row.studentGroups.flatten)
    * )
    * // Result: Math -> ["Alice", "Bob", "Charlie", "David", "Eve"], Science -> ["Frank", "Grace", "Henry"]
    *   }}}
    */
  def flatten: ScalarExpr[Seq[T]] = ScalarExpr(spark.sql.functions.flatten(new Column(array.underlying)).expr)
