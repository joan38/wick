package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.functions.count
import com.netflix.wick.functions.`*`
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class groupByTest extends FunSuite with SparkSuite:

  test("grouping and aggregating"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Anais", age = 25))
    )

    val aggregated = persons
      .groupBy(person => (age_group = person.age))
      .agg(_ => (population = count(`*`)))

    assertEquals(aggregated.dataFrame.count(), 2L)
    assertEquals(
      aggregated.dataFrame.schema,
      StructType(Array(StructField("age_group", IntegerType, true), StructField("population", LongType, false)))
    )
    assertEquals(
      aggregated.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"population":2}""",
        """{"age_group":30,"population":1}"""
      )
    )
    // Should be able to select both key and agg columns
    aggregated.select(row => (age_group = row.age_group, population = row.population))

  test("grouping and aggregating with a vector expression should not compile"):
    compileErrors("""
      val persons =
        spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Anais", age = 25)))

      persons
        .groupBy(person => (age_group = person.age))
        .agg(person => (age_sum = person.age)) // This should not compile because person.age is not an aggregation
    """)
