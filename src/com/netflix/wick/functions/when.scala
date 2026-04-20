package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Evaluates a list of `(condition, result)` branches and returns the first matching one, or the default if none match.
  *
  * Equivalent to an SQL `CASE WHEN cond1 THEN r1 WHEN cond2 THEN r2 ... ELSE default END`. For a single-branch
  * conditional, prefer [[conditional]].
  *
  * @param default
  *   the value returned when no condition matches
  * @param cases
  *   ordered `(condition, result)` pairs; the first matching condition wins
  * @tparam Result
  *   the type of the branches and default
  * @return
  *   a LinearExpr containing the matching branch's value or the default
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  * import com.netflix.wick.column.lit
  * import com.netflix.wick.functions.when
  *
  * persons.select(person =>
  *   (category = when(
  *     lit("unknown"),
  *     (person.age >= 35) -> lit("senior"),
  *     (person.age >= 18) -> lit("adult")
  *   ))
  * )
  *   }}}
  */
def when[Result](
    default: Expr[Result],
    cases: (condition: Expr[Boolean], result: Expr[Result])*
): LinearExpr[Result] =
  val first =
    spark.sql.functions.when(new Column(cases.head.condition.underlying), new Column(cases.head.result.underlying))
  LinearExpr(
    cases.tail
      .foldLeft(first)((acc, `case`) =>
        acc.when(new Column(`case`.condition.underlying), new Column(`case`.result.underlying))
      )
      .otherwise(new Column(default.underlying))
      .expr
  )

/** Evaluates `(condition, result)` branches where the result branches are scalar expressions (for use inside
  * aggregations).
  *
  * @param default
  *   the scalar value returned when no condition matches
  * @param cases
  *   ordered `(condition, result)` pairs; the first matching condition wins
  * @tparam Result
  *   the type of the branches and default
  * @return
  *   a ScalarExpr containing the matching branch's value or the default
  */
def when[Result](
    default: ScalarExpr[Result],
    cases: (condition: Expr[Boolean], result: ScalarExpr[Result])*
): ScalarExpr[Result] =
  val first =
    spark.sql.functions.when(new Column(cases.head.condition.underlying), new Column(cases.head.result.underlying))
  ScalarExpr(
    cases.tail
      .foldLeft(first)((acc, `case`) =>
        acc.when(new Column(`case`.condition.underlying), new Column(`case`.result.underlying))
      )
      .otherwise(new Column(default.underlying))
      .expr
  )
