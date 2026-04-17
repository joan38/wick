package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.lit

class aggregateTest extends FunSuite with SparkSuite:

  test("aggregate sum of grades with LinearExpr"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 88)),
        GradeBook("Bob", Seq(78, 82, 75))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        total = row.grades.aggregate(lit(0), (acc, grade) => acc + grade)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","total":263}""",
        """{"student":"Bob","total":235}"""
      )
    )

  test("aggregate max of grades with LinearExpr"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 88)),
        GradeBook("Bob", Seq(78, 82, 75))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        best = row.grades.aggregate(lit(0), (acc, grade) => when(acc, (grade > acc) -> grade))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","best":90}""",
        """{"student":"Bob","best":82}"""
      )
    )

  test("aggregate concatenate strings with ScalarExpr"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 88)),
        GradeBook("Bob", Seq(78))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        summary = row.grades.aggregate(lit("scores:"), (acc, grade) => acc ++ lit("-") ++ grade.asString)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","summary":"scores:-85-90-88"}""",
        """{"student":"Bob","summary":"scores:-78"}"""
      )
    )
