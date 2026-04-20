package com.netflix.wick
package functions

import com.netflix.wick.column.lit
import org.apache.spark
import org.apache.spark.sql.Column

/** Throws a runtime error with the given message when the expression is evaluated.
  *
  * Typically used inside a [[conditional]] or [[when]] branch to fail the job when invariant checks are violated. The
  * resulting type is `Nothing`, so it composes with any other branch type.
  *
  * @param errorMessage
  *   the error message expression
  * @return
  *   a LinearExpr[Nothing] that never produces a value
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  * import com.netflix.wick.functions.{conditional, raiseError}
  *
  * persons.select(person =>
  *   (validated = conditional(person.age >= 0, person.age, raiseError("negative age not allowed")))
  * )
  *   }}}
  */
def raiseError(errorMessage: Expr[String]): LinearExpr[Nothing] = LinearExpr(
  spark.sql.functions.raise_error(new Column(errorMessage.underlying)).expr
)

/** Throws a runtime error with the given scalar message when the expression is evaluated.
  *
  * Used inside aggregations where the message is itself a scalar expression.
  *
  * @param errorMessage
  *   the error message scalar expression
  * @return
  *   a ScalarExpr[Nothing] that never produces a value
  */
def raiseError(errorMessage: ScalarExpr[String]): ScalarExpr[Nothing] = ScalarExpr(
  spark.sql.functions.raise_error(new Column(errorMessage.underlying)).expr
)

/** Throws a runtime error with a literal String message when the expression is evaluated.
  *
  * @param errorMessage
  *   the literal error message
  * @return
  *   a ScalarExpr[Nothing] that never produces a value
  */
def raiseError(errorMessage: String): ScalarExpr[Nothing] = ScalarExpr(
  spark.sql.functions.raise_error(new Column(lit(errorMessage).underlying)).expr
)
