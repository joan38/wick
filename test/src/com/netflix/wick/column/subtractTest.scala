package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.sum
import com.netflix.wick.functions.count

class subtractTest extends FunSuite with SparkSuite:

  test("Int - literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_minus_5 = nullable(person.age.? - 5)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_minus_5", IntegerType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_minus_5":25}""",
        """{"age_minus_5":20}""",
        """{"age_minus_5":30}"""
      )
    )

  test("Int - Expr[Int]"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_diff = nullable(person.age.? - person.age.?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_diff", IntegerType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_diff":0}""",
        """{"age_diff":0}""",
        """{"age_diff":0}"""
      )
    )

  test("Int - literal Long yields Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_minus_long = nullable(person.age.? - 10L)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_minus_long", LongType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_minus_long":20}""",
        """{"age_minus_long":15}""",
        """{"age_minus_long":25}"""
      )
    )

  test("Int - literal Double yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_minus_double = nullable(person.age.? - 0.5)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_minus_double", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_minus_double":29.5}""",
        """{"age_minus_double":24.5}""",
        """{"age_minus_double":34.5}"""
      )
    )

  test("unary minus"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (neg_age = nullable(-person.age.?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("neg_age", IntegerType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"neg_age":-30}""",
        """{"neg_age":-25}""",
        """{"neg_age":-35}"""
      )
    )

  test("ScalarExpr Long - literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_minus_10 = nullable(sum(person.age).? - 10)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_minus_10", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_minus_10":15}""",
        """{"age_group":30,"sum_minus_10":50}"""
      )
    )

  test("ScalarExpr Long - ScalarExpr Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_minus_count = nullable(sum(person.age).? - count(person.age).?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_minus_count", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_minus_count":24}""",
        """{"age_group":30,"sum_minus_count":58}"""
      )
    )
