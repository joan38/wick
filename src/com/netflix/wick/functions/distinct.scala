package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Removes duplicate values from an array.
    *
    * This function returns a new array containing only the unique elements from the input array. The order of elements
    * is preserved, keeping the first occurrence of each element. Returns null if the input array is null.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the array with duplicates removed
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Movie(title: String, genres: Seq[String])
    * val movies = spark.createDataSeq(Seq(
    *   Movie("Action Film", Seq("Action", "Drama", "Action", "Thriller")),
    *   Movie("Comedy Show", Seq("Comedy", "Romance", "Comedy", "Comedy"))
    * ))
    *
    * // Get unique genres for each movie
    * val uniqueGenres = movies.select(row =>
    *   (title = row.title, unique_genres = row.genres.distinct)
    * )
    * // Result: Action Film -> ["Action", "Drama", "Thriller"], Comedy Show -> ["Comedy", "Romance"]
    *   }}}
    */
  def distinct: LinearExpr[Seq[T]] = LinearExpr(
    spark.sql.functions.array_distinct(new Column(array.underlying)).expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Removes duplicate values from an array.
    *
    * This function returns a new array containing only the unique elements from the input array. The order of elements
    * is preserved, keeping the first occurrence of each element. Returns null if the input array is null.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the array with duplicates removed
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Movie(title: String, genres: Seq[String])
    * val movies = spark.createDataSeq(Seq(
    *   Movie("Action Film", Seq("Action", "Drama", "Action", "Thriller")),
    *   Movie("Comedy Show", Seq("Comedy", "Romance", "Comedy", "Comedy"))
    * ))
    *
    * // Get unique genres for each movie
    * val uniqueGenres = movies.select(row =>
    *   (title = row.title, unique_genres = row.genres.distinct)
    * )
    * // Result: Action Film -> ["Action", "Drama", "Thriller"], Comedy Show -> ["Comedy", "Romance"]
    *   }}}
    */
  def distinct: ScalarExpr[Seq[T]] = ScalarExpr(
    spark.sql.functions.array_distinct(new Column(array.underlying)).expr
  )
