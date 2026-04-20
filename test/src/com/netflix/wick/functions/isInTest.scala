package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class isInTest extends FunSuite with SparkSuite:

  test("isIn filters rows matching the value set"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = 25),
        Person("Charlie", age = 35),
        Person("Dave", age = 40)
      )
    )

    val result = persons.filter(person => person.name.isIn("Alice", "Charlie"))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("isIn as a column expression"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person => (name = person.name, is_allowed = person.name.isIn("Alice", "Charlie")))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","is_allowed":true}""",
        """{"name":"Bob","is_allowed":false}"""
      )
    )

  test("isIn with no matching values"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.filter(person => person.name.isIn("Xavier", "Yasmin"))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq.empty
    )
