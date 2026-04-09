package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.expressions.Literal

/** Creates a Col[T] of literal value. The type T is inferred from the value.
  *
  * This function creates a column with a constant literal value that can be used in selections, filters, joins, and
  * other operations. The literal value will be the same for every row.
  *
  * @param value
  *   the constant value to be wrapped in a Col (can be any type supported by Spark)
  * @tparam T
  *   the type of the literal value, automatically inferred
  * @return
  *   a Col[T] representing the literal value that can be used in column operations
  *
  * @example
  *   {{{ import com.netflix.wick.{*, given}
  *
  * case class Person(name: String, age: Int) val persons = spark.createDataSeq(Seq(Person("Alice", age = 30),
  * Person("Bob", age = 15)))
  *
  * // Add a constant column val withConstant = persons.select(row => ( name = row.name, age = row.age, status =
  * lit("active"), version = lit(1) ))
  *
  * // Use in filters val filtered = persons.filter(row => row.age > lit(25))
  */
def lit[T](value: T): ScalarExpr[T] = ScalarExpr(Literal(value))
