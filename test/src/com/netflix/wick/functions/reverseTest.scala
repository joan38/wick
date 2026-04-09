package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class reverseTest extends FunSuite with SparkSuite:

  test("reverse array elements"):
    val playlists = spark.createDataSeq(
      Seq(
        Playlist("Rock", Seq("Song A", "Song B", "Song C")),
        Playlist("Pop", Seq("Hit 1", "Hit 2", "Hit 3", "Hit 4")),
        Playlist("Jazz", Seq("Track 1"))
      )
    )

    val result = playlists.select(row =>
      (
        playlist = row.name,
        reversed_songs = row.songs.reverse
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"playlist":"Rock","reversed_songs":["Song C","Song B","Song A"]}""",
        """{"playlist":"Pop","reversed_songs":["Hit 4","Hit 3","Hit 2","Hit 1"]}""",
        """{"playlist":"Jazz","reversed_songs":["Track 1"]}"""
      )
    )

  test("reverse numeric arrays"):
    val sequences = spark.createDataSeq(
      Seq(
        NumberSequence("Ascending", Seq(1, 2, 3, 4, 5)),
        NumberSequence("Even", Seq(2, 4, 6, 8)),
        NumberSequence("Single", Seq(42))
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        reversed_numbers = row.numbers.reverse
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Ascending","reversed_numbers":[5,4,3,2,1]}""",
        """{"name":"Even","reversed_numbers":[8,6,4,2]}""",
        """{"name":"Single","reversed_numbers":[42]}"""
      )
    )

  test("reverse with scalar expressions in aggregation"):
    case class Student(name: String, score1: Int, score2: Int, score3: Int)

    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 85, 90, 78),
        Student("Bob", 92, 88, 95)
      )
    )

    val result = students
      .agg(row =>
        (
          score_totals = array(sum(row.score1), sum(row.score2), sum(row.score3))
        )
      )
      .select(row =>
        (
          reversed_totals = row.score_totals.reverse
        )
      )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"reversed_totals":[173,178,177]}""")
    )

  test("reverse empty array"):
    val emptySequences = spark.createDataSeq(
      Seq(
        NumberSequence("Empty", Seq())
      )
    )

    val result = emptySequences.select(row =>
      (
        name = row.name,
        reversed_numbers = row.numbers.reverse
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Empty","reversed_numbers":[]}""")
    )

  test("reverse with null array"):
    case class NullableSequence(name: String, numbers: Seq[Int] | Null)

    val sequences = spark.createDataSeq(
      Seq(
        NullableSequence("Valid", Seq(1, 2, 3)),
        NullableSequence("Null", null)
      )
    )

    val result = sequences.select(row =>
      (
        name = row.name,
        reversed_numbers = row.numbers.orElse(array[Int]()).reverse
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Valid","reversed_numbers":[3,2,1]}""",
        """{"name":"Null","reversed_numbers":[]}"""
      )
    )

  test("reverse large array"):
    val largeSequence = spark.createDataSeq(
      Seq(
        NumberSequence("Large", (1 to 10).toSeq)
      )
    )

    val result = largeSequence.select(row =>
      (
        name = row.name,
        reversed_numbers = row.numbers.reverse
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Large","reversed_numbers":[10,9,8,7,6,5,4,3,2,1]}""")
    )

  test("reverse preserves duplicates"):
    val duplicateSequence = spark.createDataSeq(
      Seq(
        NumberSequence("Duplicates", Seq(1, 2, 2, 3, 2, 1))
      )
    )

    val result = duplicateSequence.select(row =>
      (
        name = row.name,
        reversed_numbers = row.numbers.reverse
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Duplicates","reversed_numbers":[1,2,3,2,2,1]}""")
    )
