package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class containsTest extends FunSuite with SparkSuite:

  test("array contains extension method"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 88)),
        GradeBook("Bob", Seq(78, 82, 80)),
        GradeBook("Charlie", Seq(92, 85, 90))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        has_perfect = row.grades.contains(100),
        has_good = row.grades.contains(90)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","has_perfect":false,"has_good":true}""",
        """{"student":"Bob","has_perfect":false,"has_good":false}""",
        """{"student":"Charlie","has_perfect":false,"has_good":true}"""
      )
    )

  test("array contains with null values"):
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
        has_ninety = row.grades.orElse(array[Int]()).contains(90)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","has_ninety":true}""",
        """{"student":"Bob","has_ninety":false}""",
        """{"student":"Charlie","has_ninety":false}"""
      )
    )
