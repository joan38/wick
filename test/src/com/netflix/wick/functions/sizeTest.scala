package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class sizeTest extends FunSuite with SparkSuite:

  test("array size extension method"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 88)),
        GradeBook("Bob", Seq(78, 82)),
        GradeBook("Charlie", Seq(92, 85, 90, 87, 89))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        grade_count = row.grades.size
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","grade_count":3}""",
        """{"student":"Bob","grade_count":2}""",
        """{"student":"Charlie","grade_count":5}"""
      )
    )

  test("array size with null values"):
    case class NullableGradeBook(student: String, grades: Seq[Int] | Null)

    val gradeBooks = spark.createDataSeq(
      Seq(
        NullableGradeBook("Alice", Seq(85, 90, 88)),
        NullableGradeBook("Bob", null),
        NullableGradeBook("Charlie", Seq())
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        grade_count = row.grades.orElse(array[Int]()).size
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","grade_count":3}""",
        """{"student":"Bob","grade_count":0}""",
        """{"student":"Charlie","grade_count":0}"""
      )
    )

  test("array size with scalar expressions"):
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
          array_size = row.combined_scores.size
        )
      )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"array_size":3}""")
    )
