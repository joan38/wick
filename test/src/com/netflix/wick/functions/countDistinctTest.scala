package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class countDistinctTest extends FunSuite with SparkSuite:

  test("countDistinct on single column"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = 25),
        Person("Alice", age = 40),
        Person("Charlie", age = 25)
      )
    )

    val result = persons.agg(person => (distinct_names = countDistinct(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"distinct_names":3}""")
    )

  test("countDistinct on multiple columns counts distinct combinations"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = 25),
        Person("Alice", age = 30),
        Person("Alice", age = 40)
      )
    )

    val result = persons.agg(person => (distinct_pairs = countDistinct(person.name, person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"distinct_pairs":3}""")
    )

  test("countDistinct ignores nulls"):
    val personsWithNulls = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = null),
        Person("Charlie", age = null),
        Person("Dave", age = 30)
      )
    )

    val result = personsWithNulls.agg(person => (distinct_ages = countDistinct(person.age)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"distinct_ages":1}""")
    )

  test("countDistinct with groupBy"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Alice", age = 30),
        Person("Bob", age = 25),
        Person("Charlie", age = 25),
        Person("Dave", age = 25)
      )
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (unique_names = countDistinct(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"unique_names":3}""",
        """{"age_group":30,"unique_names":1}"""
      )
    )

  test("countDistinct on empty dataset"):
    val empty  = spark.createDataSeq(Seq.empty[Person])
    val result = empty.agg(person => (distinct_names = countDistinct(person.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"distinct_names":0}""")
    )
