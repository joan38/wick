package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.sum

class moduloTest extends FunSuite with SparkSuite:

  test("modulo Int by literal Int"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val ageMod10 = persons.select(person => (age_mod_10 = nullable(person.age.? % 10)))

    assertEquals(
      ageMod10.dataFrame.schema,
      StructType(Array(StructField("age_mod_10", IntegerType, nullable = true)))
    )
    assertEquals(
      ageMod10.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_mod_10":0}""",
        """{"age_mod_10":5}""",
        """{"age_mod_10":5}"""
      )
    )

  test("modulo Int by Expr[Int]"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val ageMod = persons.select(person => (age_mod = nullable(person.age.? % person.age.?)))

    assertEquals(
      ageMod.dataFrame.schema,
      StructType(Array(StructField("age_mod", IntegerType, nullable = true)))
    )
    assertEquals(
      ageMod.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_mod":0}""",
        """{"age_mod":0}""",
        """{"age_mod":0}"""
      )
    )

  test("modulo Int by literal Long"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val ageMod = persons.select(person => (age_mod_long = nullable(person.age.? % 7L)))

    assertEquals(
      ageMod.dataFrame.schema,
      StructType(Array(StructField("age_mod_long", LongType, nullable = true)))
    )
    assertEquals(
      ageMod.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_mod_long":2}""",
        """{"age_mod_long":4}""",
        """{"age_mod_long":0}"""
      )
    )

  test("modulo ScalarExpr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val ageTotals = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_mod_100 = nullable(sum(person.age).? % 100)))

    assertEquals(
      ageTotals.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_mod_100", LongType, nullable = true)
        )
      )
    )
    assertEquals(
      ageTotals.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_mod_100":25}""",
        """{"age_group":30,"sum_mod_100":60}"""
      )
    )
