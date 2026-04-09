package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.sum
import com.netflix.wick.functions.count

class addTest extends FunSuite with SparkSuite:

  test("Int + literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_plus_5 = nullable(person.age.? + 5)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_plus_5", IntegerType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_plus_5":35}""",
        """{"age_plus_5":30}""",
        """{"age_plus_5":40}"""
      )
    )

  test("Int + Expr[Int]"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_doubled = nullable(person.age.? + person.age.?)))

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

  test("Int + literal Long yields Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_plus_long = nullable(person.age.? + 100L)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_plus_long", LongType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_plus_long":130}""",
        """{"age_plus_long":125}""",
        """{"age_plus_long":135}"""
      )
    )

  test("Int + literal Double yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_plus_double = nullable(person.age.? + 0.5)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_plus_double", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_plus_double":30.5}""",
        """{"age_plus_double":25.5}""",
        """{"age_plus_double":35.5}"""
      )
    )

  test("ScalarExpr Int + literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_plus_10 = nullable(sum(person.age).? + 10)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_plus_10", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_plus_10":35}""",
        """{"age_group":30,"sum_plus_10":70}"""
      )
    )

  test("ScalarExpr Long + ScalarExpr Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_plus_count = nullable(sum(person.age).? + count(person.age).?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_plus_count", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_plus_count":26}""",
        """{"age_group":30,"sum_plus_count":62}"""
      )
    )
