package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.lit
import com.netflix.wick.functions.coalesce

class coalesceTest extends FunSuite with SparkSuite:

  test("coalesce should use fallback"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (n = person.name, a = coalesce(person.age, lit(0))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"n":"Alice","a":30}""",
        """{"n":"Bob","a":0}""",
        """{"n":"Charlie","a":35}"""
      )
    )

  test("coalesce should use multiple fallbacks with the last not nullable"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (n = person.name, a = coalesce(person.age, person.age, lit(0))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"n":"Alice","a":30}""",
        """{"n":"Bob","a":0}""",
        """{"n":"Charlie","a":35}"""
      )
    )

  test("coalesce should not get confused with coalesce on DataSeq"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    persons.select(person => (n = person.name, a = coalesce(person.age, lit(0)))).coalesce(1)
