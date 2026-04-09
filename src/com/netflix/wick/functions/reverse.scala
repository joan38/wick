package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Reverses the order of elements in an array.
    *
    * This function returns a new array with the elements in reverse order. The first element becomes the last, the last
    * becomes the first, and so on. Returns null if the input array is null.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the reversed array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Playlist(name: String, songs: Seq[String])
    * val playlists = spark.createDataSeq(Seq(
    *   Playlist("Rock", Seq("Song A", "Song B", "Song C")),
    *   Playlist("Pop", Seq("Hit 1", "Hit 2", "Hit 3", "Hit 4"))
    * ))
    *
    * // Reverse the order of songs in each playlist
    * val reversedPlaylists = playlists.select(row =>
    *   (playlist = row.name, reversed_songs = row.songs.reverse)
    * )
    * // Result: Rock -> ["Song C", "Song B", "Song A"], Pop -> ["Hit 4", "Hit 3", "Hit 2", "Hit 1"]
    *   }}}
    */
  def reverse: LinearExpr[Seq[T]] = LinearExpr(spark.sql.functions.reverse(new Column(array.underlying)).expr)

extension [T](array: ScalarExpr[Seq[T]])
  /** Reverses the order of elements in an array.
    *
    * This function returns a new array with the elements in reverse order. The first element becomes the last, the last
    * becomes the first, and so on. Returns null if the input array is null.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the reversed array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Playlist(name: String, songs: Seq[String])
    * val playlists = spark.createDataSeq(Seq(
    *   Playlist("Rock", Seq("Song A", "Song B", "Song C")),
    *   Playlist("Pop", Seq("Hit 1", "Hit 2", "Hit 3", "Hit 4"))
    * ))
    *
    * // Reverse the order of songs in each playlist
    * val reversedPlaylists = playlists.select(row =>
    *   (playlist = row.name, reversed_songs = row.songs.reverse)
    * )
    * // Result: Rock -> ["Song C", "Song B", "Song A"], Pop -> ["Hit 4", "Hit 3", "Hit 2", "Hit 1"]
    *   }}}
    */
  def reverse: ScalarExpr[Seq[T]] = ScalarExpr(spark.sql.functions.reverse(new Column(array.underlying)).expr)
