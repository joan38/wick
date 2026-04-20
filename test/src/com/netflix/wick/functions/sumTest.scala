package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class sumTest extends FunSuite with SparkSuite:

  test("sum across all rows"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (total_age = sum(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total_age":90}""")
    )

  test("sum ignores null values"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 20))
    )

    val result = personsWithNulls.agg(person => (total_age = sum(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total_age":50}""")
    )

  test("sum with groupBy"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Anais", age = 30),
        Person("Bob", age = 25),
        Person("Charlie", age = 25)
      )
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_age = sum(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_age":50}""",
        """{"age_group":30,"sum_age":60}"""
      )
    )

  test("sum on empty dataset returns null"):
    val empty  = spark.createDataSeq(Seq.empty[Person])
    val result = empty.agg(person => (total_age = sum(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total_age":null}""")
    )
