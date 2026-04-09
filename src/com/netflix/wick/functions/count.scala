package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Aggregate function: returns the number of items in a group.
  *
  * This function counts the number of non-null values in the specified column within each group. When used with
  * groupBy, it counts items per group. When used without groupBy, it counts all items.
  *
  * @param col
  *   the column to count non-null values for
  * @return
  *   a Col[Int] representing the count of non-null items in the group
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
  * // Count all persons
  * val totalCount = persons.agg(row => (total = count(row.name)))
  * // Result: 3
  *
  * // Count persons by age group
  * val countByAge = persons
  *   .groupBy(row => (age_group = conditional(row.age > 30, "senior", "junior")))
  *   .agg(row => (population = count(row.name)))
  * // Result: senior -> 1, junior -> 2
  *   }}}
  */
def count(expr: Expr[?]): ScalarExpr[Long] = ScalarExpr(
  spark.sql.functions.count(new Column(expr.underlying)).expr
)
