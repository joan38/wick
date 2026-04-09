package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class sortedTest extends FunSuite with SparkSuite:

  test("sorted array in ascending order"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78, 92)),
        GradeBook("Bob", Seq(95, 82, 88)),
        GradeBook("Charlie", Seq(70, 85, 82, 90, 75))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        sorted_grades = row.grades.sorted(asc = true)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","sorted_grades":[78,85,90,92]}""",
        """{"student":"Bob","sorted_grades":[82,88,95]}""",
        """{"student":"Charlie","sorted_grades":[70,75,82,85,90]}"""
      )
    )

  test("sorted array in descending order"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78, 92)),
        GradeBook("Bob", Seq(95, 82, 88))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        sorted_grades_desc = row.grades.sorted(asc = false)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","sorted_grades_desc":[92,90,85,78]}""",
        """{"student":"Bob","sorted_grades_desc":[95,88,82]}"""
      )
    )

  test("sorted array with default order"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78, 92))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        sorted_grades = row.grades.sorted
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","sorted_grades":[78,85,90,92]}""")
    )

  test("sorted array with scalar expressions"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 85, 90, 88),
        Student("Bob", 78, 82, 80)
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
          sorted_totals = row.combined_scores.sorted(asc = true)
        )
      )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"sorted_totals":[163,168,172]}""")
    )

  test("sorted empty array"):
    case class EmptyGradeBook(student: String, grades: Seq[Int])

    val gradeBooks = spark.createDataSeq(
      Seq(
        EmptyGradeBook("Alice", Seq())
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        sorted_grades = row.grades.sorted(asc = true)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","sorted_grades":[]}""")
    )
