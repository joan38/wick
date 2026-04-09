package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.lit

class explodeTest extends FunSuite with SparkSuite:

  test("explode Seq[String] produces one row per element"):
    val playlists = spark.createDataSeq(
      Seq(
        Playlist("Rock", Seq("Song A", "Song B", "Song C")),
        Playlist("Pop", Seq("Song D", "Song E"))
      )
    )

    val result = playlists.select(row => (playlist = row.name, song = row.songs.explode))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"playlist":"Rock","song":"Song A"}""",
        """{"playlist":"Rock","song":"Song B"}""",
        """{"playlist":"Rock","song":"Song C"}""",
        """{"playlist":"Pop","song":"Song D"}""",
        """{"playlist":"Pop","song":"Song E"}"""
      )
    )

  test("explode Seq[Int] produces one row per element"):
    val sequences = spark.createDataSeq(
      Seq(
        NumberSequence("evens", Seq(2, 4, 6)),
        NumberSequence("odds", Seq(1, 3))
      )
    )

    val result = sequences.select(row => (name = row.name, num = row.numbers.explode))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"evens","num":2}""",
        """{"name":"evens","num":4}""",
        """{"name":"evens","num":6}""",
        """{"name":"odds","num":1}""",
        """{"name":"odds","num":3}"""
      )
    )

  test("explode empty array produces no rows for that input"):
    val playlists = spark.createDataSeq(
      Seq(
        Playlist("Full", Seq("Song A", "Song B")),
        Playlist("Empty", Seq())
      )
    )

    val result = playlists.select(row => (playlist = row.name, song = row.songs.explode))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"playlist":"Full","song":"Song A"}""",
        """{"playlist":"Full","song":"Song B"}"""
      )
    )

  test("explode result is a LinearExpr usable in subsequent operations"):
    val sequences = spark.createDataSeq(Seq(NumberSequence("a", Seq(1, 2, 3))))

    // explode first, then apply arithmetic in a second select
    val exploded = sequences.select(row => (name = row.name, num = row.numbers.explode))
    val result   = exploded.select(row => (name = row.name, num10 = row.num + lit(10)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"a","num10":11}""",
        """{"name":"a","num10":12}""",
        """{"name":"a","num10":13}"""
      )
    )
