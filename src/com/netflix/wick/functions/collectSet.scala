package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

/** Aggregate function: collects unique values from a group into a set.
  *
  * This function collects all distinct non-null values from the specified column into a set, removing duplicates. The
  * order of elements in the resulting set is not guaranteed. When used with groupBy, it collects unique values per
  * group. When used without groupBy, it collects all unique values into a single set.
  *
  * @tparam T
  *   the type of the values to collect
  * @param expr
  *   the column expression to collect values from
  * @return
  *   a ScalarExpr[Set[T]] representing the collected set of unique values
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Rating(userId: String, movieId: String, rating: Int)
  * val ratings = spark.createDataSeq(Seq(
  *   Rating("user1", "movie1", 5),
  *   Rating("user1", "movie2", 4),
  *   Rating("user1", "movie3", 5),
  *   Rating("user2", "movie1", 3),
  *   Rating("user2", "movie2", 3)
  * ))
  *
  * // Collect unique ratings per user
  * val uniqueRatingsPerUser = ratings
  *   .groupBy(row => (user = row.userId))
  *   .agg(row => (unique_ratings = collectSet(row.rating)))
  * // Result: user1 -> {5, 4}, user2 -> {3}
  *
  * // Collect all unique ratings
  * val allUniqueRatings = ratings.agg(row => (unique_ratings = collectSet(row.rating)))
  * // Result: {5, 4, 3}
  *   }}}
  */
def collectSet[T](expr: Expr[T]): ScalarExpr[Set[T]] = ScalarExpr(
  spark.sql.functions.collect_set(new Column(expr.underlying)).expr
)
