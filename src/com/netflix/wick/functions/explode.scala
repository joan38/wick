package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Produces one row per element of the array, duplicating the other columns of each input row.
    *
    * Rows whose array is empty or null are dropped from the output.
    *
    * @return
    *   a LinearExpr[T] containing a single element per produced row
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Playlist(name: String, songs: Seq[String])
    * val playlists = spark.createDataSeq(Seq(Playlist("Rock", Seq("A", "B", "C"))))
    *
    * playlists.select(row => (playlist = row.name, song = row.songs.explode))
    * // Result: one row per song
    *   }}}
    */
  def explode: LinearExpr[T] = LinearExpr(
    spark.sql.functions.explode(new Column(array.underlying)).expr
  )
