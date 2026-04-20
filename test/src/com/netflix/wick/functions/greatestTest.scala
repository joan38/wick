package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class greatestTest extends FunSuite with SparkSuite:

  test("greatest of several Int columns"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", score1 = 85, score2 = 90, score3 = 78),
        Student("Bob", score1 = 70, score2 = 60, score3 = 95)
      )
    )

    val result = students.select(row => (name = row.name, best = greatest(row.score1, row.score2, row.score3)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","best":90}""",
        """{"name":"Bob","best":95}"""
      )
    )

  test("greatest with lit values"):
    val students = spark.createDataSeq(
      Seq(Student("Alice", score1 = 85, score2 = 90, score3 = 78))
    )

    val result = students.select(row => (name = row.name, capped = greatest(row.score1, lit(100))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","capped":100}""")
    )

  test("greatest of ScalarExpr in agg"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", score1 = 85, score2 = 90, score3 = 78),
        Student("Bob", score1 = 70, score2 = 60, score3 = 95)
      )
    )

    val result = students.agg(row => (top = greatest(max(row.score1), max(row.score2), max(row.score3))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"top":95}""")
    )
