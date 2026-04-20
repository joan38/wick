package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class conditionalTest extends FunSuite with SparkSuite:

  test("conditional with Expr branches"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person =>
      (
        name = person.name,
        category = conditional(person.age.orElse(0) > 30, lit("senior"), lit("junior"))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","category":"junior"}""",
        """{"name":"Bob","category":"junior"}""",
        """{"name":"Charlie","category":"senior"}"""
      )
    )

  test("conditional with null values in condition"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = null), Person("Bob", age = 25), Person("Charlie", age = 40))
    )

    val result = personsWithNulls.select(person =>
      (
        name = person.name,
        category = conditional(person.age.orElse(0) >= 30, lit("adult"), lit("young"))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","category":"young"}""",
        """{"name":"Bob","category":"young"}""",
        """{"name":"Charlie","category":"adult"}"""
      )
    )

  test("conditional with ScalarExpr branches in agg"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result =
      persons.agg(person => (label = conditional(max(person.age).orElse(0) > 30, min(person.name), max(person.name))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"label":"Alice"}""")
    )

  test("conditional with numeric branches"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person =>
      (
        name = person.name,
        bonus = conditional(person.age.orElse(0) >= 30, lit(100), lit(0))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","bonus":100}""",
        """{"name":"Bob","bonus":0}"""
      )
    )
