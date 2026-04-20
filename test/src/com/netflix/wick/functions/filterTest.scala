package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.functions.filter
import com.netflix.wick.model.*

class filterTest extends FunSuite with SparkSuite:

  test("filter keeps only matching elements"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78, 92)),
        GradeBook("Bob", Seq(70, 60, 95))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        passing = row.grades.filter(_ >= lit(80))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","passing":[85,90,92]}""",
        """{"student":"Bob","passing":[95]}"""
      )
    )

  test("filter on empty array returns empty array"):
    val gradeBooks = spark.createDataSeq(
      Seq(GradeBook("Alice", Seq()))
    )

    val result = gradeBooks.select(row => (student = row.student, passing = row.grades.filter(_ >= lit(80))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","passing":[]}""")
    )

  test("filter with predicate that matches nothing"):
    val gradeBooks = spark.createDataSeq(
      Seq(GradeBook("Alice", Seq(10, 20, 30)))
    )

    val result = gradeBooks.select(row => (student = row.student, top = row.grades.filter(_ > lit(100))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","top":[]}""")
    )
