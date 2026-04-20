package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Returns one of two expressions depending on a boolean condition.
  *
  * Equivalent to an SQL `CASE WHEN condition THEN ... ELSE ... END` with a single branch. For multiple branches, use
  * [[when]] instead.
  *
  * @param condition
  *   the boolean expression to evaluate
  * @param then
  *   the expression to return when the condition is true
  * @param else
  *   the expression to return when the condition is false
  * @tparam Result
  *   the type of the returned branches
  * @return
  *   a LinearExpr containing the selected branch value
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  * import com.netflix.wick.column.lit
  *
  * case class Person(name: String, age: Int)
  * val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15)))
  *
  * persons.select(person => (category = conditional(person.age >= 18, lit("adult"), lit("minor"))))
  * // Result: "adult", "minor"
  *   }}}
  */
def conditional[Result](
    condition: Expr[Boolean],
    `then`: Expr[Result],
    `else`: Expr[Result]
): LinearExpr[Result] = LinearExpr(
  spark.sql.functions
    .when(new Column(condition.underlying), new Column(`then`.underlying))
    .otherwise(new Column(`else`.underlying))
    .expr
)

/** Returns one of two scalar expressions depending on a boolean condition.
  *
  * Used inside aggregations where all branches are scalar.
  *
  * @param condition
  *   the boolean expression to evaluate
  * @param then
  *   the expression to return when the condition is true
  * @param else
  *   the expression to return when the condition is false
  * @tparam Result
  *   the type of the returned branches
  * @return
  *   a ScalarExpr containing the selected branch value
  */
def conditional[Result](
    condition: ScalarExpr[Boolean],
    `then`: ScalarExpr[Result],
    `else`: ScalarExpr[Result]
): ScalarExpr[Result] = ScalarExpr(
  spark.sql.functions
    .when(new Column(condition.underlying), new Column(`then`.underlying))
    .otherwise(new Column(`else`.underlying))
    .expr
)
