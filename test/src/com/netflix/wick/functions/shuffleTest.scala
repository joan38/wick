package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class shuffleTest extends FunSuite with SparkSuite:

  test("shuffle array"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(1, 2, 3, 4, 5))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        shuffled_grades = row.grades.shuffle,
        original_size = row.grades.size
      )
    )

    val collected = result.dataFrame.collect().map(_.json).toSeq
    // We can't predict the exact shuffle result, but we can verify structure
    assert(collected.size == 1)
    assert(collected.head.contains(""""student":"Alice""""))
    assert(collected.head.contains(""""original_size":5"""))
    assert(collected.head.contains("shuffled_grades"))

  test("shuffle array with scalar expressions"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 10, 20, 30),
        Student("Bob", 40, 50, 60)
      )
    )

    val result = students
      .agg(row =>
        (
          combined_scores = array(sum(row.score1), sum(row.score2), sum(row.score3))
        )
      )
      .select(row =>
        (
          shuffled_totals = row.combined_scores.shuffle,
          original_size = row.combined_scores.size
        )
      )

    val collected = result.dataFrame.collect().map(_.json).toSeq
    // Verify structure without checking exact shuffle order
    assert(collected.size == 1)
    assert(collected.head.contains(""""original_size":3"""))
    assert(collected.head.contains("shuffled_totals"))

  test("shuffle empty array"):
    case class EmptyGradeBook(student: String, grades: Seq[Int])

    val gradeBooks = spark.createDataSeq(
      Seq(
        EmptyGradeBook("Alice", Seq())
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        shuffled_grades = row.grades.shuffle
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","shuffled_grades":[]}""")
    )
