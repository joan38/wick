package com.netflix.wick
package functions

import com.netflix.wick.column.Numeric
import org.apache.spark
import org.apache.spark.sql.Column

/** Returns the absolute value of a numeric expression.
  *
  * This function calculates the absolute value (magnitude) of numeric values, converting negative numbers to their
  * positive equivalent while keeping positive numbers unchanged. Works with both integer and floating-point types.
  *
  * @param col
  *   the numeric column to calculate the absolute value for
  * @return
  *   a LinearExpr[T] representing the absolute value
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Transaction(id: String, amount: Double)
  * val transactions = spark.createDataSeq(Seq(
  *   Transaction("t1", -100.0),
  *   Transaction("t2", 50.0),
  *   Transaction("t3", -25.5)
  * ))
  *
  * // Calculate absolute values of transaction amounts
  * val absoluteAmounts = transactions.select(row => (amount = abs(row.amount)))
  * // Result: 100.0, 50.0, 25.5
  *
  * // Use in aggregations
  * val totalAbsoluteAmount = transactions.agg(row => (total = abs(sum(row.amount))))
  * // Result: 175.5
  *   }}}
  */
def abs[T <: Numeric | Null](expr: Expr[T]): LinearExpr[T] = LinearExpr(
  spark.sql.functions.abs(new Column(expr.underlying)).expr
)

def abs[T <: Numeric | Null](expr: ScalarExpr[T]): ScalarExpr[T] = ScalarExpr(
  spark.sql.functions.abs(new Column(expr.underlying)).expr
)
