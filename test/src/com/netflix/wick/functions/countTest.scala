package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.count
import com.netflix.wick.model.*

class countTest extends FunSuite with SparkSuite:

  test("count all non-null values"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (total = count(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total":3}""")
    )

  test("count ignores null values"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = null))
    )

    val result = personsWithNulls.agg(person => (non_null_ages = count(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"non_null_ages":1}""")
    )

  test("count with groupBy"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = 25),
        Person("Charlie", age = 30),
        Person("Dave", age = 25)
      )
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (population = count(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"population":2}""",
        """{"age_group":30,"population":2}"""
      )
    )

  test("count on empty dataset"):
    val empty  = spark.createDataSeq(Seq.empty[Person])
    val result = empty.agg(person => (total = count(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total":0}""")
    )
