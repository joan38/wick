package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class existsTest extends FunSuite with SparkSuite:

  test("array exists extension method"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 88)),
        GradeBook("Bob", Seq(78, 82, 75)),
        GradeBook("Charlie", Seq(92, 85, 90))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        has_high_grade = row.grades.exists(grade => grade > 90),
        has_low_grade = row.grades.exists(grade => grade < 80)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","has_high_grade":false,"has_low_grade":false}""",
        """{"student":"Bob","has_high_grade":false,"has_low_grade":true}""",
        """{"student":"Charlie","has_high_grade":true,"has_low_grade":false}"""
      )
    )
