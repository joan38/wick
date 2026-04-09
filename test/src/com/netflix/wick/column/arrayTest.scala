package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.functions.sum
import com.netflix.wick.functions.exists

class arrayTest extends FunSuite with SparkSuite:

  test("creating array from multiple columns"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 85, 90, 88),
        Student("Bob", 78, 82, 80),
        Student("Charlie", 92, 85, 90)
      )
    )

    val result = students.select(row =>
      (
        name = row.name,
        scores = array(row.score1, row.score2, row.score3)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","scores":[85,90,88]}""",
        """{"name":"Bob","scores":[78,82,80]}""",
        """{"name":"Charlie","scores":[92,85,90]}"""
      )
    )

  test("creating array from scalar expressions in aggregation"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 85, 90, 88),
        Student("Bob", 78, 82, 80),
        Student("Charlie", 92, 85, 90)
      )
    )

    val result = students.agg(row =>
      (
        totals = array(sum(row.score1), sum(row.score2), sum(row.score3))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"totals":[255,257,258]}""")
    )

  test("empty array creation"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 85, 90, 88)
      )
    )

    val result = students.select(row =>
      (
        name = row.name,
        empty_array = array[Int]()
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","empty_array":[]}""")
    )

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
