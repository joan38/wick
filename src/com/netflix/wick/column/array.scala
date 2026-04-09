package com.netflix.wick
package column

import org.apache.spark
import org.apache.spark.sql.Column
import scala.annotation.targetName

/** Creates an array from the given expressions.
  *
  * This function constructs an array from a variable number of expressions. All expressions must have the same element
  * type T.
  *
  * @param cols
  *   the expressions to include in the array
  * @return
  *   a LinearExpr[Seq[T]] representing the constructed array
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Person(name: String, score1: Int, score2: Int, score3: Int)
  * val people = spark.createDataSeq(Seq(
  *   Person("Alice", 85, 90, 88),
  *   Person("Bob", 78, 82, 80)
  * ))
  *
  * // Create an array from multiple score expressions
  * val scoresSeq = people.select(row => array(row.score1, row.score2, row.score3))
  * // Result: [85, 90, 88], [78, 82, 80]
  *   }}}
  */
def array[T](cols: Expr[T]*): LinearExpr[Seq[T]] = LinearExpr(
  spark.sql.functions.array(cols.map(col => new Column(col.underlying))*).expr
)

@targetName("arrayScalar")
def array[T](cols: ScalarExpr[T]*): ScalarExpr[Seq[T]] = ScalarExpr(
  spark.sql.functions.array(cols.map(col => new Column(col.underlying))*).expr
)
