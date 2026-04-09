package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar

/** Wildcard column representing "all columns" in select operations.
  *
  * This special column acts as a shorthand for selecting all available columns from a DataSeq. It's particularly useful
  * when you want to include all columns without explicitly naming each one, commonly used in aggregation functions like
  * count.
  *
  * @return
  *   a Col[Nothing] that represents all columns in the current context
  *
  * @note
  *   The Col[Nothing] type indicates this is a special marker column that doesn't represent a specific data type but
  *   rather a placeholder for "all columns".
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  * import com.netflix.wick.functions.count
  *
  * case class Person(name: String, age: Int, city: String)
  * val persons = spark.createDataSeq(Seq(
  *   Person("Alice", age = 30, city = "NYC"),
  *   Person("Bob", age = 15, city = "LA")
  * ))
  *
  * // Count all rows using the wildcard
  * val totalCount = persons.agg(row => (total = count(`*`)))
  * // Result: 2
  *
  * // Count by groups using the wildcard
  * val countByCity = persons
  *   .groupBy(row => (city = row.city))
  *   .agg(row => (population = count(*)))
  * // Result: NYC -> 1, LA -> 1
  *   }}}
  */
def * = LinearExpr[UnresolvedStar](UnresolvedStar(None))
