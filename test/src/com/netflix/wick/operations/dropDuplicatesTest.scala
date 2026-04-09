package com.netflix.wick.operations

import munit.FunSuite
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class dropDuplicatesTest extends FunSuite with SparkSuite:

  test("dropDuplicates removes rows with duplicate key"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Alice", age = 99))
    )

    val result = persons.dropDuplicates(_.name)

    assertEquals(result.dataFrame.count(), 2L)
    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("name")).toSet,
      Set("Alice", "Bob")
    )

  test("dropDuplicates keeps first occurrence"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Alice", age = 99), Person("Bob", age = 25))
    )

    val result = persons.dropDuplicates(_.name)

    assertEquals(
      result.dataFrame.collect().map(r => r.getAs[String]("name") -> r.get(1)).toMap,
      Map("Alice" -> 30, "Bob" -> 25)
    )

  test("dropDuplicates on numeric key"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 30), Person("Charlie", age = 25))
    )

    val result = persons.dropDuplicates(_.age)

    assertEquals(result.dataFrame.count(), 2L)

  test("dropDuplicates on already unique dataset returns all rows"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.dropDuplicates(_.name)

    assertEquals(result.dataFrame.count(), 3L)

  test("dropDuplicates on empty dataset returns empty dataset"):
    val persons = spark.createDataSeq(Seq.empty[Person])

    val result = persons.dropDuplicates(_.name)

    assertEquals(result.dataFrame.count(), 0L)

  test("dropDuplicates preserves schema"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Alice", age = 99))
    )

    val result = persons.dropDuplicates(_.name)

    assertEquals(result.dataFrame.schema, spark.createDataSeq(Seq(Person("Alice", age = 30))).dataFrame.schema)
