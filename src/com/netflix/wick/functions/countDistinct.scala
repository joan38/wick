package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Aggregate function: returns the number of distinct items in a group.
  *
  * This function counts the number of unique combinations across the specified columns. When multiple columns are
  * provided, it counts distinct combinations of values across all columns.
  *
  * @param col
  *   the first column to count distinct values for
  * @param cols
  *   additional columns to include in the distinct count (optional)
  * @return
  *   a Col[Int] representing the count of distinct items or combinations in the group
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Person(name: String, age: Int, city: String)
  * val persons = spark.createDataSeq(Seq(
  *   Person("Alice", age = 30, city = "NYC"),
  *   Person("Bob", age = 25, city = "NYC"),
  *   Person("Alice", age = 30, city = "LA")
  * ))
  *
  * // Count distinct names
  * val distinctNames = persons.agg(row => (distinct_names = countDistinct(row.name)))
  * // Result: 2 (Alice, Bob)
  *
  * // Count distinct name-age combinations
  * val distinctPairs = persons.agg(row => (distinct_pairs = countDistinct(row.name, row.age)))
  * // Result: 3 (all combinations are unique)
  *   }}}
  */
def countDistinct(exprs: Expr[?]*): ScalarExpr[Int] = ScalarExpr(
  spark.sql.functions
    .countDistinct(new Column(exprs.head.underlying), exprs.tail.map(expr => new Column(expr.underlying))*)
    .expr
)
