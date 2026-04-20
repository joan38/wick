package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.expressions.Cast
import com.netflix.wick.column.Numeric
import org.apache.spark.sql.types.DataTypes

extension [A <: Numeric | Null](expr: Expr[A])
  /** Casts a numeric expression to its String representation.
    *
    * @return
    *   a LinearExpr[String] containing the string representation of the numeric value
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    * import com.netflix.wick.functions.asString
    *
    * case class Person(name: String, age: Int)
    * val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))
    *
    * persons.select(person => (age_text = person.age.asString))
    * // Result: "30"
    *   }}}
    */
  def asString: LinearExpr[String] = LinearExpr(Cast(expr.underlying, DataTypes.StringType))

extension [A <: Numeric | Null](expr: ScalarExpr[A])
  /** Casts a numeric scalar expression to its String representation.
    *
    * @return
    *   a ScalarExpr[String] containing the string representation of the numeric value
    */
  def asString: ScalarExpr[String] = ScalarExpr(Cast(expr.underlying, DataTypes.StringType))
