package com.netflix.wick
package functions

import org.apache.spark.sql.Column

extension [T](expr: Expr[T])
  /** Tests whether the expression's value is contained in a given set of literals.
    *
    * @param values
    *   the literals to test membership against
    * @return
    *   a LinearExpr[Boolean] that is true when the value matches any of the inputs
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Person(name: String, age: Int)
    * val persons = spark.createDataSeq(Seq(Person("Alice", 30), Person("Bob", 25)))
    *
    * persons.filter(person => person.name.isIn("Alice", "Charlie"))
    * // Result: keeps only Alice
    *   }}}
    */
  def isIn(values: T*): LinearExpr[Boolean] = LinearExpr(
    new Column(expr.underlying).isin(values*).expr
  )

extension [T](expr: ScalarExpr[T])
  /** Tests whether a scalar expression's value is contained in a given set of literals.
    *
    * @param values
    *   the literals to test membership against
    * @return
    *   a ScalarExpr[Boolean] that is true when the value matches any of the inputs
    */
  def isIn(values: T*): ScalarExpr[Boolean] = ScalarExpr(
    new Column(expr.underlying).isin(values*).expr
  )
