package com.netflix.wick.functions

import com.netflix.wick.column.lit
import com.netflix.wick.functions.{asString, first}
import com.netflix.wick.model.*
import com.netflix.wick.{*, given}
import munit.FunSuite
import org.apache.spark.sql.types.*

class stringsTest extends FunSuite with SparkSuite:

  test("string concatenation with ++ operator (Expr + Expr)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val greetings = persons.select(person => (greeting = person.name ++ lit(" says hello")))

    assertEquals(
      greetings.dataFrame.schema,
      StructType(Array(StructField("greeting", StringType, nullable = true)))
    )
    assertEquals(
      greetings.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"greeting":"Alice says hello"}""",
        """{"greeting":"Bob says hello"}""",
        """{"greeting":"Charlie says hello"}"""
      )
    )

  test("string concatenation with ++ operator (Expr + String)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val greetings = persons.select(person => (greeting = person.name ++ " says hello"))

    assertEquals(
      greetings.dataFrame.schema,
      StructType(Array(StructField("greeting", StringType, nullable = true)))
    )
    assertEquals(
      greetings.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"greeting":"Alice says hello"}""",
        """{"greeting":"Bob says hello"}""",
        """{"greeting":"Charlie says hello"}"""
      )
    )

  test("string concatenation with ++ operator (String + Expr)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val greetings = persons.select(person => (greeting = "Hello " ++ person.name))

    assertEquals(
      greetings.dataFrame.schema,
      StructType(Array(StructField("greeting", StringType, nullable = true)))
    )
    assertEquals(
      greetings.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"greeting":"Hello Alice"}""",
        """{"greeting":"Hello Bob"}""",
        """{"greeting":"Hello Charlie"}"""
      )
    )

  test("string concatenation with ++ operator (String + ScalarExpr)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedNames = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (greeting = "Hello " ++ first(person.name)))

    assertEquals(
      groupedNames.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("greeting", StringType, nullable = true)
        )
      )
    )
    assertEquals(
      groupedNames.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"greeting":"Hello Bob"}""",
        """{"age_group":30,"greeting":"Hello Alice"}"""
      )
    )

  test("string concatenation with ++ operator (String + String)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val staticGreeting = persons.select(_ => (greeting = "Hello" ++ " World"))

    assertEquals(
      staticGreeting.dataFrame.schema,
      StructType(Array(StructField("greeting", StringType, nullable = false)))
    )
    assertEquals(
      staticGreeting.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"greeting":"Hello World"}""",
        """{"greeting":"Hello World"}""",
        """{"greeting":"Hello World"}"""
      )
    )

  test("complex string concatenation"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val complex = persons.select(person =>
      (message = "Name: " ++ person.name ++ ", Age: " ++ nullable(person.age.?.asString).orElse("unknown"))
    )

    assertEquals(
      complex.dataFrame.schema,
      StructType(Array(StructField("message", StringType, nullable = true)))
    )
    assertEquals(
      complex.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"message":"Name: Alice, Age: 30"}""",
        """{"message":"Name: Bob, Age: 25"}""",
        """{"message":"Name: Charlie, Age: 35"}"""
      )
    )
