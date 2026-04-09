package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.sum
import com.netflix.wick.functions.count

class compareTest extends FunSuite with SparkSuite:

  test("less than literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val isYoung = persons.select(person => (is_young = nullable(person.age.? < 30)))

    assertEquals(
      isYoung.dataFrame.schema,
      StructType(Array(StructField("is_young", BooleanType, nullable = true)))
    )
    assertEquals(
      isYoung.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_young":false}""",
        """{"is_young":true}""",
        """{"is_young":false}"""
      )
    )

  test("less than Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (younger_than_charlie = nullable(person.age.? < 35)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"younger_than_charlie":true}""",
        """{"younger_than_charlie":true}""",
        """{"younger_than_charlie":false}"""
      )
    )

  test("greater than literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val isOld = persons.select(person => (is_old = nullable(person.age.? > 30)))

    assertEquals(
      isOld.dataFrame.schema,
      StructType(Array(StructField("is_old", BooleanType, nullable = true)))
    )
    assertEquals(
      isOld.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_old":false}""",
        """{"is_old":false}""",
        """{"is_old":true}"""
      )
    )

  test("greater than Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (older_than_bob = nullable(person.age.? > 25)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"older_than_bob":true}""",
        """{"older_than_bob":false}""",
        """{"older_than_bob":true}"""
      )
    )

  test("less than or equal literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (at_most_30 = nullable(person.age.? <= 30)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("at_most_30", BooleanType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"at_most_30":true}""",
        """{"at_most_30":true}""",
        """{"at_most_30":false}"""
      )
    )

  test("greater than or equal literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.select(person => (at_least_30 = nullable(person.age.? >= 30)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("at_least_30", BooleanType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"at_least_30":true}""",
        """{"at_least_30":false}""",
        """{"at_least_30":true}"""
      )
    )

  test("ScalarExpr less than literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_under_100 = nullable(sum(person.age).? < 100)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_under_100", BooleanType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_under_100":true}""",
        """{"age_group":30,"sum_under_100":true}"""
      )
    )

  test("ScalarExpr greater than ScalarExpr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (sum_gt_count = nullable(sum(person.age).? > count(person.age).?)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("sum_gt_count", BooleanType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.json).map(_.json).toSeq,
      Seq(
        """{"age_group":25,"sum_gt_count":true}""",
        """{"age_group":30,"sum_gt_count":true}"""
      )
    )
