package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.functions.when
import com.netflix.wick.model.*

class whenTest extends FunSuite with SparkSuite:

  test("when with single condition"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person =>
      (name = person.name, category = when(lit("unknown"), (person.age.orElse(0) > 30) -> lit("senior")))
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","category":"unknown"}""",
        """{"name":"Bob","category":"unknown"}""",
        """{"name":"Charlie","category":"senior"}"""
      )
    )

  test("when with multiple conditions"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person =>
      (
        name = person.name,
        category = when(
          lit("unknown"),
          (person.age.orElse(0) >= 35) -> lit("senior"),
          (person.age.orElse(0) >= 30) -> lit("adult"),
          (person.age.orElse(0) >= 18) -> lit("young adult")
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","category":"adult"}""",
        """{"name":"Bob","category":"young adult"}""",
        """{"name":"Charlie","category":"senior"}"""
      )
    )

  test("when with no matching conditions uses default"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person =>
      (name = person.name, category = when(lit("default"), (person.age.orElse(0) > 50) -> lit("elder")))
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","category":"default"}""",
        """{"name":"Bob","category":"default"}"""
      )
    )

  test("when with null handling"):
    val personsWithNulls = spark.createDataSeq(Seq(Person("Alice", age = null), Person("Bob", age = 25)))

    val result = personsWithNulls.select(person =>
      (
        name = person.name,
        category = when(
          lit("no age"),
          (person.age.orElse(-1) === -1) -> lit("null age"),
          (person.age.orElse(0) < 30)    -> lit("young")
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","category":"null age"}""",
        """{"name":"Bob","category":"young"}"""
      )
    )
