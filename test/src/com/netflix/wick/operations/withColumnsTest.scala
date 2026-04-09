package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*
import com.netflix.wick.functions.++

class withColumnsTest extends FunSuite with SparkSuite:

  test("adding single column"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val withDoubleAge = persons.withColumns(person => (double_age = nullable(person.age.? * 2)))
    assertEquals(withDoubleAge.dataFrame.count(), 3L)
    assertEquals(
      withDoubleAge.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, true),
          StructField("age", IntegerType, true),
          StructField("double_age", IntegerType, true)
        )
      )
    )
    assertEquals(
      withDoubleAge.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30,"double_age":60}""",
        """{"name":"Bob","age":15,"double_age":30}""",
        """{"name":"Charlie","age":35,"double_age":70}"""
      )
    )

  test("adding multiple columns"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val withAgeInfo = persons.withColumns(person =>
      (
        double_age = nullable(person.age.? * 2),
        triple_age = nullable(person.age.? * 3),
        is_adult = nullable(person.age.? >= 18)
      )
    )
    assertEquals(withAgeInfo.dataFrame.count(), 3L)
    assertEquals(
      withAgeInfo.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, true),
          StructField("age", IntegerType, true),
          StructField("double_age", IntegerType, true),
          StructField("triple_age", IntegerType, true),
          StructField("is_adult", BooleanType, true)
        )
      )
    )
    assertEquals(
      withAgeInfo.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30,"double_age":60,"triple_age":90,"is_adult":true}""",
        """{"name":"Bob","age":15,"double_age":30,"triple_age":45,"is_adult":false}""",
        """{"name":"Charlie","age":35,"double_age":70,"triple_age":105,"is_adult":true}"""
      )
    )

  test("chaining withColumns"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val result = persons
      .withColumns(person => (double_age = nullable(person.age.? * 2)))
      .withColumns(row => (quadruple_age = nullable(row.double_age.? * 2)))

    assertEquals(result.dataFrame.count(), 3L)
    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, true),
          StructField("age", IntegerType, true),
          StructField("double_age", IntegerType, true),
          StructField("quadruple_age", IntegerType, true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30,"double_age":60,"quadruple_age":120}""",
        """{"name":"Bob","age":15,"double_age":30,"quadruple_age":60}""",
        """{"name":"Charlie","age":35,"double_age":70,"quadruple_age":140}"""
      )
    )

  test("withColumns with string transformations"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val result = persons.withColumns(person => (greeting = "Hello " ++ person.name))

    assertEquals(result.dataFrame.count(), 3L)
    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, true),
          StructField("age", IntegerType, true),
          StructField("greeting", StringType, true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30,"greeting":"Hello Alice"}""",
        """{"name":"Bob","age":15,"greeting":"Hello Bob"}""",
        """{"name":"Charlie","age":35,"greeting":"Hello Charlie"}"""
      )
    )

  test("withColumns on non-existent column should fail compilation"):
    compileErrors("""
      val persons =
        spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35)))

      persons.withColumns(person => (result = person.non_existent_column))
    """)
