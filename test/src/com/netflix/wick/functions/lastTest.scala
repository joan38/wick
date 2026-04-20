package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.last
import com.netflix.wick.model.*

class lastTest extends FunSuite with SparkSuite:

  test("last returns the last value in a group"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 30), Person("Charlie", age = 25))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (last_name = last(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"last_name":"Charlie"}""",
        """{"age_group":30,"last_name":"Bob"}"""
      )
    )

  test("last across all rows without grouping"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (last_name = last(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"last_name":"Charlie"}""")
    )

  test("last with ignoreNulls returns last non-null"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = null))
    )

    val result = personsWithNulls.agg(person => (last_age = last(person.age, ignoreNulls = true)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"last_age":25}""")
    )

  test("last without ignoreNulls may return null"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null))
    )

    val result = personsWithNulls.agg(person => (last_age = last(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"last_age":null}""")
    )
