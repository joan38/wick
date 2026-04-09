package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Aggregate function: collects all values from a group into a list (array).
  *
  * This function collects all non-null values from the specified column into an array, preserving the order of
  * insertion. Unlike collectSet, this function keeps duplicate values. When used with groupBy, it collects values per
  * group. When used without groupBy, it collects all values into a single array.
  *
  * @tparam T
  *   the type of the values to collect
  * @param expr
  *   the column expression to collect values from
  * @return
  *   a ScalarExpr[Seq[T]] representing the collected list of values
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Transaction(userId: String, amount: Int)
  * val transactions = spark.createDataSeq(Seq(
  *   Transaction("user1", 100),
  *   Transaction("user1", 200),
  *   Transaction("user2", 150),
  *   Transaction("user1", 50),
  *   Transaction("user2", 300)
  * ))
  *
  * // Collect all transaction amounts per user
  * val amountsPerUser = transactions
  *   .groupBy(row => (user = row.userId))
  *   .agg(row => (amounts = collectList(row.amount)))
  * // Result: user1 -> [100, 200, 50], user2 -> [150, 300]
  *
  * // Collect all amounts into a single list
  * val allAmounts = transactions.agg(row => (all_amounts = collectList(row.amount)))
  * // Result: [100, 200, 150, 50, 300]
  *   }}}
  */
def collectList[T](expr: Expr[T]): ScalarExpr[Seq[T]] = ScalarExpr(
  spark.sql.functions.collect_list(new Column(expr.underlying)).expr
)
