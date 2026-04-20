package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class getTest extends FunSuite with SparkSuite:

  test("get with literal position (1-based)"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78)),
        GradeBook("Bob", Seq(70, 60, 95))
      )
    )

    val result = gradeBooks.select(row => (student = row.student, first_grade = row.grades.get(1)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","first_grade":85}""",
        """{"student":"Bob","first_grade":70}"""
      )
    )

  test("get with Expr position"):
    val gradeBooks = spark.createDataSeq(
      Seq(GradeBook("Alice", Seq(85, 90, 78)))
    )

    val result = gradeBooks.select(row => (student = row.student, grade = row.grades.get(lit(2))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","grade":90}""")
    )

  test("get with out-of-range position returns null"):
    val gradeBooks = spark.createDataSeq(
      Seq(GradeBook("Alice", Seq(85, 90)))
    )

    val result = gradeBooks.select(row => (student = row.student, missing = row.grades.get(10)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","missing":null}""")
    )

  test("get with position 0 should not compile"):
    compileErrors("""
      import com.netflix.wick.model.*
      val gradeBooks = spark.createDataSeq(Seq(GradeBook("Alice", Seq(85, 90))))
      gradeBooks.select(row => (v = row.grades.get(0)))
    """)
