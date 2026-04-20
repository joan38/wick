package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class raiseErrorTest extends FunSuite with SparkSuite:

  test("raiseError with literal String fails when branch is hit"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = -1))
    )

    val result = persons.select(person =>
      (
        name = person.name,
        validated_age = conditional(
          person.age.orElse(0) >= 0,
          person.age.orElse(0),
          raiseError("negative age not allowed")
        )
      )
    )

    val ex = intercept[RuntimeException](result.dataFrame.collect())
    assert(
      ex.getMessage.contains("negative age not allowed"),
      s"expected error to mention the message, got: ${ex.getMessage}"
    )

  test("raiseError branch is not triggered when condition is false"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person =>
      (
        name = person.name,
        validated_age = conditional(
          person.age.orElse(0) >= 0,
          person.age.orElse(0),
          raiseError("negative age not allowed")
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","validated_age":30}""",
        """{"name":"Bob","validated_age":25}"""
      )
    )

  test("raiseError with Expr[String] message"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = -1)))

    val result = persons.select(person =>
      (
        checked = conditional(
          person.age.orElse(0) >= 0,
          lit("ok"),
          raiseError("invalid for " ++ person.name)
        )
      )
    )

    val ex = intercept[RuntimeException](result.dataFrame.collect())
    assert(
      ex.getMessage.contains("invalid for Alice"),
      s"expected error to mention 'invalid for Alice', got: ${ex.getMessage}"
    )
