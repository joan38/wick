package com.netflix.wick
package functions

import com.netflix.wick.column.Orderable
import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the greatest value across the given expressions for each row.
  *
  * Nulls are ignored unless all inputs are null, in which case the result is null. Only orderable types are accepted.
  *
  * @param exprs
  *   the expressions to compare
  * @tparam T
  *   the orderable type of the expressions
  * @return
  *   a LinearExpr containing the maximum value among the inputs per row
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Student(name: String, score1: Int, score2: Int, score3: Int)
  * val students = spark.createDataSeq(Seq(Student("Alice", 85, 90, 78)))
  *
  * students.select(row => (best = greatest(row.score1, row.score2, row.score3)))
  * // Result: 90
  *   }}}
  */
def greatest[T: Orderable](exprs: Expr[T]*): LinearExpr[T] = LinearExpr(
  spark.sql.functions.greatest(exprs.map(expr => new Column(expr.underlying))*).expr
)

/** Returns the greatest value across the given scalar expressions.
  *
  * Used inside aggregations where all operands are scalar.
  *
  * @param exprs
  *   the scalar expressions to compare
  * @tparam T
  *   the orderable type of the expressions
  * @return
  *   a ScalarExpr containing the maximum of the inputs
  */
def greatest[T: Orderable](exprs: ScalarExpr[T]*): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.greatest(exprs.map(expr => new Column(expr.underlying))*).expr
)
