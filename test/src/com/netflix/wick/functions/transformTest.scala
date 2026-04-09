package com.netflix.wick.functions

import com.netflix.wick.model.*
import munit.FunSuite
import com.netflix.wick.{*, given}

class transformTest extends FunSuite with SparkSuite:

  test("transform using 2 Expr to format a String"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person => (formatted = transform(person.name() + "-" + person.age.orElse(0)())))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"formatted":"Alice-30"}""",
        """{"formatted":"Bob-25"}"""
      )
    )

  test("transform using 3 Expr to format a String"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15)))

    val result = persons.select(person =>
      (formatted = transform(s"${person.name()}-${person.age.orElse(0)()}-${person.age.orElse(0)() > 18}"))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"formatted":"Alice-30-true"}""",
        """{"formatted":"Bob-15-false"}"""
      )
    )

  test("transform using type alias to Expr"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    // Make sure transform works when LinearExpr is a function parameter type
    def prefixed(name: LinearExpr[String]): LinearExpr[String] =
      transform("Hello " + name())

    val result = persons.select(person => (greeting = prefixed(person.name)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"greeting":"Hello Alice"}""",
        """{"greeting":"Hello Bob"}"""
      )
    )
