package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.first
import com.netflix.wick.model.*

class firstTest extends FunSuite with SparkSuite:

  test("first returns the first value in a group"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 30), Person("Charlie", age = 25))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (first_name = first(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"first_name":"Charlie"}""",
        """{"age_group":30,"first_name":"Alice"}"""
      )
    )

  test("first across all rows without grouping"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (first_name = first(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"first_name":"Alice"}""")
    )

  test("first with ignoreNulls returns first non-null"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = null), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = personsWithNulls.agg(person => (first_age = first(person.age, ignoreNulls = true)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"first_age":25}""")
    )

  test("first without ignoreNulls may return null"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = null), Person("Bob", age = 25))
    )

    val result = personsWithNulls.agg(person => (first_age = first(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"first_age":null}""")
    )
