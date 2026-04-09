package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.sum
import com.netflix.wick.functions.count

class divideTest extends FunSuite with SparkSuite:

  test("Int / literal Int yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_halved = nullable(person.age.? / lit(2))))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_halved", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_halved":15.0}""",
        """{"age_halved":12.5}""",
        """{"age_halved":17.5}"""
      )
    )

  test("Int / Expr[Int] yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_div_self = nullable(person.age.? / person.age.?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_div_self", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_div_self":1.0}""",
        """{"age_div_self":1.0}""",
        """{"age_div_self":1.0}"""
      )
    )

  test("Int / literal Long yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_div_long = nullable(person.age.? / 5L)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_div_long", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_div_long":6.0}""",
        """{"age_div_long":5.0}""",
        """{"age_div_long":7.0}"""
      )
    )

  test("Int / literal Double yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (age_div_double = nullable(person.age.? / 2.0)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("age_div_double", DoubleType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_div_double":15.0}""",
        """{"age_div_double":12.5}""",
        """{"age_div_double":17.5}"""
      )
    )

  test("ScalarExpr Long / literal Int yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_halved = nullable(sum(person.age).? / lit(2))))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_halved", DoubleType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_halved":12.5}""",
        """{"age_group":30,"sum_halved":30.0}"""
      )
    )

  test("ScalarExpr Long / ScalarExpr Long yields Double"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_div_count = nullable(sum(person.age).? / count(person.age).?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_div_count", DoubleType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_div_count":25.0}""",
        """{"age_group":30,"sum_div_count":30.0}"""
      )
    )
