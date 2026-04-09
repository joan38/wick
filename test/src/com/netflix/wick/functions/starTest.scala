package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.functions.count
import com.netflix.wick.functions.`*`
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class starTest extends FunSuite with SparkSuite:

  test("using * in max as max(*) should not compile"):
    compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.agg(_ => (max_all = max(`*`)))
    """)

  test("using * in min as min(*) should not compile"):
    compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.agg(_ => (min_all = min(`*`)))
    """)

  test("using * with count in aggregation"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(_ => (total_count = count(`*`)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total_count":3}""")
    )

  test("using * with count in grouped aggregation"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Anais", age = 25))
    )

    val aggregated = persons.groupBy(person => (age_group = person.age)).agg(_ => (population = count(`*`)))
    assertEquals(
      aggregated.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"population":2}""",
        """{"age_group":30,"population":1}"""
      )
    )

  test("using * with count on empty dataset"):
    val emptyPersons = spark.createDataSeq(Seq.empty[Person])
    val result       = emptyPersons.agg(_ => (total_count = count(`*`)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total_count":0}""")
    )

  test("using * with count on dataset with null values"):
    case class PersonWithNullableName(name: Option[String], age: Int)
    val personsWithNulls = spark.createDataSeq(
      Seq(
        PersonWithNullableName(Some("Alice"), age = 30),
        PersonWithNullableName(None, age = 25),
        PersonWithNullableName(Some("Charlie"), age = 35)
      )
    )

    val result = personsWithNulls.agg(_ => (total_count = count(`*`)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"total_count":3}""")
    )

  test("using * in countDistinct should not work"):
    compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.agg(_ => (distinct_count = countDistinct(`*`)))
    """)
