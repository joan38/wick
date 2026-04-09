package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class distinctTest extends FunSuite with SparkSuite:

  test("distinct removes duplicate strings"):
    val playlists = spark.createDataSeq(
      Seq(
        Playlist("Rock", Seq("Song A", "Song B", "Song A", "Song C", "Song B")),
        Playlist("Pop", Seq("Hit 1", "Hit 2", "Hit 1", "Hit 1")),
        Playlist("Jazz", Seq("Track 1", "Track 1", "Track 1"))
      )
    )

    val result = playlists.select(row =>
      (
        playlist = row.name,
        unique_songs = row.songs.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"playlist":"Rock","unique_songs":["Song A","Song B","Song C"]}""",
        """{"playlist":"Pop","unique_songs":["Hit 1","Hit 2"]}""",
        """{"playlist":"Jazz","unique_songs":["Track 1"]}"""
      )
    )

  test("distinct removes duplicate numbers"):
    val sequences = spark.createDataSeq(
      Seq(
        NumberSequence("Duplicates", Seq(1, 2, 3, 2, 1, 4, 3)),
        NumberSequence("AllSame", Seq(5, 5, 5, 5)),
        NumberSequence("NoDuplicates", Seq(1, 2, 3, 4))
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Duplicates","unique_numbers":[1,2,3,4]}""",
        """{"name":"AllSame","unique_numbers":[5]}""",
        """{"name":"NoDuplicates","unique_numbers":[1,2,3,4]}"""
      )
    )

  test("distinct with scalar expressions in aggregation"):
    case class Rating(userId: String, movieId: String, rating: Int)

    val ratings = spark.createDataSeq(
      Seq(
        Rating("user1", "movie1", 5),
        Rating("user1", "movie2", 4),
        Rating("user2", "movie1", 5),
        Rating("user2", "movie3", 5),
        Rating("user3", "movie4", 3)
      )
    )

    val result = ratings
      .agg(row =>
        (
          all_ratings = collectList(row.rating)
        )
      )
      .select(row =>
        (
          unique_ratings = row.all_ratings.distinct
        )
      )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"unique_ratings":[5,4,3]}""")
    )

  test("distinct preserves order of first occurrence"):
    val sequences = spark.createDataSeq(
      Seq(
        NumberSequence("OrderTest", Seq(3, 1, 4, 1, 5, 9, 2, 6, 5, 3))
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"OrderTest","unique_numbers":[3,1,4,5,9,2,6]}""")
    )

  test("distinct on empty array"):
    val emptySequences = spark.createDataSeq(
      Seq(
        NumberSequence("Empty", Seq())
      )
    )

    val result = emptySequences.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Empty","unique_numbers":[]}""")
    )

  test("distinct with null array"):
    case class NullableSequence(name: String, numbers: Seq[Int] | Null)

    val sequences = spark.createDataSeq(
      Seq(
        NullableSequence("Valid", Seq(1, 2, 2, 3)),
        NullableSequence("Null", null)
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.orElse(array[Int]()).distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Valid","unique_numbers":[1,2,3]}""",
        """{"name":"Null","unique_numbers":[]}"""
      )
    )

  test("distinct on array with single element"):
    val sequences = spark.createDataSeq(
      Seq(
        NumberSequence("Single", Seq(42))
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Single","unique_numbers":[42]}""")
    )

  test("distinct on array with all duplicates"):
    val playlists = spark.createDataSeq(
      Seq(
        Playlist("Repeat", Seq("Same", "Same", "Same", "Same", "Same"))
      )
    )

    val result = playlists.select(row =>
      (
        playlist = row.name,
        unique_songs = row.songs.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"playlist":"Repeat","unique_songs":["Same"]}""")
    )

  test("distinct large array with many duplicates"):
    val largeSequence = spark.createDataSeq(
      Seq(
        NumberSequence("Large", Seq(1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 4, 5, 4, 5))
      )
    )

    val result = largeSequence.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Large","unique_numbers":[1,2,3,4,5]}""")
    )

  test("distinct with consecutive duplicates"):
    val sequences = spark.createDataSeq(
      Seq(
        NumberSequence("Consecutive", Seq(1, 1, 2, 2, 2, 3, 4, 4, 5))
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        unique_numbers = row.numbers.distinct
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Consecutive","unique_numbers":[1,2,3,4,5]}""")
    )
