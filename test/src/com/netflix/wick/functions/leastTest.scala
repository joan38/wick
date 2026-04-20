package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class leastTest extends FunSuite with SparkSuite:

  test("least of several Int columns"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", score1 = 85, score2 = 90, score3 = 78),
        Student("Bob", score1 = 70, score2 = 60, score3 = 95)
      )
    )

    val result = students.select(row => (name = row.name, worst = least(row.score1, row.score2, row.score3)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","worst":78}""",
        """{"name":"Bob","worst":60}"""
      )
    )

  test("least with lit values caps to floor"):
    val students = spark.createDataSeq(
      Seq(Student("Alice", score1 = 85, score2 = 90, score3 = 78))
    )

    val result = students.select(row => (name = row.name, floored = least(row.score1, lit(50))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","floored":50}""")
    )

  test("least of ScalarExpr in agg"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", score1 = 85, score2 = 90, score3 = 78),
        Student("Bob", score1 = 70, score2 = 60, score3 = 95)
      )
    )

    val result = students.agg(row => (lowest = least(min(row.score1), min(row.score2), min(row.score3))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"lowest":60}""")
    )
