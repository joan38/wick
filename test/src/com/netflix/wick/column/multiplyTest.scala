package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.sum
import com.netflix.wick.functions.count

class multiplyTest extends FunSuite with SparkSuite:

  test("Int * literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_doubled = nullable(person.age.? * 2)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_doubled", IntegerType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_doubled":60}""",
        """{"age_doubled":50}""",
        """{"age_doubled":70}"""
      )
    )

  test("Int * Expr[Int]"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_squared = nullable(person.age.? * person.age.?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_squared", IntegerType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_squared":900}""",
        """{"age_squared":625}""",
        """{"age_squared":1225}"""
      )
    )

  test("Int * literal Long yields Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_times_long = nullable(person.age.? * 10L)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_times_long", LongType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_times_long":300}""",
        """{"age_times_long":250}""",
        """{"age_times_long":350}"""
      )
    )

  test("Int * literal Double yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_times_double = nullable(person.age.? * 1.5)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_times_double", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_times_double":45.0}""",
        """{"age_times_double":37.5}""",
        """{"age_times_double":52.5}"""
      )
    )

  test("ScalarExpr Int * literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_doubled = nullable(sum(person.age).? * 2)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_doubled", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_doubled":50}""",
        """{"age_group":30,"sum_doubled":120}"""
      )
    )

  test("ScalarExpr Long * ScalarExpr Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_times_count = nullable(sum(person.age).? * count(person.age).?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_times_count", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_times_count":25}""",
        """{"age_group":30,"sum_times_count":120}"""
      )
    )
