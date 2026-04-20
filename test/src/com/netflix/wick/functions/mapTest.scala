package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class mapTest extends FunSuite with SparkSuite:

  test("map applies a transformation to each element"):
    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78)),
        GradeBook("Bob", Seq(70, 60, 95))
      )
    )

    val result = gradeBooks.select(row => (student = row.student, curved = row.grades.map(_ + lit(5))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","curved":[90,95,83]}""",
        """{"student":"Bob","curved":[75,65,100]}"""
      )
    )

  test("map on empty array"):
    val gradeBooks = spark.createDataSeq(
      Seq(GradeBook("Alice", Seq()))
    )

    val result = gradeBooks.select(row => (student = row.student, curved = row.grades.map(_ + lit(5))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","curved":[]}""")
    )

  test("map can change element type"):
    val gradeBooks = spark.createDataSeq(
      Seq(GradeBook("Alice", Seq(85, 90, 78)))
    )

    val result = gradeBooks.select(row => (student = row.student, doubled = row.grades.map(_ * lit(2))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"student":"Alice","doubled":[170,180,156]}""")
    )
