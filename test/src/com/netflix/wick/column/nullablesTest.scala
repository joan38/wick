package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.{lit, orElse}
import com.netflix.wick.functions.{count, sum, when, `*`}

class nullablesTest extends FunSuite with SparkSuite:

  test("isNull"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val ageIsNull = persons.select(person => (is_null = person.age.isNull))

    assertEquals(
      ageIsNull.dataFrame.schema,
      StructType(Array(StructField("is_null", BooleanType, nullable = false)))
    )
    assertEquals(
      ageIsNull.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_null":false}""",
        """{"is_null":true}""",
        """{"is_null":false}"""
      )
    )

  test("isNotNull"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val ageIsNotNull = persons.select(person => (is_not_null = person.age.isNotNull))

    assertEquals(
      ageIsNotNull.dataFrame.schema,
      StructType(Array(StructField("is_not_null", BooleanType, nullable = false)))
    )
    assertEquals(
      ageIsNotNull.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_not_null":true}""",
        """{"is_not_null":false}""",
        """{"is_not_null":true}"""
      )
    )

  test("isNull with ScalarExpr in aggregation"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = null),
        Person("Charlie", age = 20),
        Person("David", age = null),
        Person("Eve", age = 25)
      )
    )

    val groupedByName = persons
      .groupBy(person => (name = person.name))
      .agg(person => (sum_age_is_null = sum(person.age).isNull))

    assertEquals(
      groupedByName.dataFrame.collect().map(_.json).toSet,
      Set(
        """{"name":"Alice","sum_age_is_null":false}""",
        """{"name":"David","sum_age_is_null":true}""",
        """{"name":"Charlie","sum_age_is_null":false}""",
        """{"name":"Bob","sum_age_is_null":true}""",
        """{"name":"Eve","sum_age_is_null":false}"""
      )
    )

  test("isNotNull with ScalarExpr in aggregation"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = null),
        Person("Charlie", age = 20),
        Person("David", age = null),
        Person("Eve", age = 25)
      )
    )

    val groupedByName = persons
      .groupBy(person => (name = person.name))
      .agg(person => (sum_age_is_not_null = sum(person.age).isNotNull))

    assertEquals(
      groupedByName.dataFrame.collect().map(_.json).toSet,
      Set(
        """{"name":"Alice","sum_age_is_not_null":true}""",
        """{"name":"David","sum_age_is_not_null":false}""",
        """{"name":"Charlie","sum_age_is_not_null":true}""",
        """{"name":"Bob","sum_age_is_not_null":false}""",
        """{"name":"Eve","sum_age_is_not_null":true}"""
      )
    )

  test("orElse"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val ageOrZero = persons.select(person => (age_or_zero = person.age.orElse(0)))

    assertEquals(
      ageOrZero.dataFrame.schema,
      StructType(Array(StructField("age_or_zero", IntegerType, nullable = false)))
    )
    assertEquals(
      ageOrZero.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_or_zero":30}""",
        """{"age_or_zero":0}""",
        """{"age_or_zero":35}"""
      )
    )

  test("orElse should not compile if the field is not nullable"):
    compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
      )

      persons.select(person => (name = person.name.orElse("unknown")))
    """)

  test("orElse with groupBy and sum aggregation"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = null),
        Person("Charlie", age = 20),
        Person("David", age = null),
        Person("Eve", age = 25)
      )
    )

    val ageStats = persons
      .groupBy(person => (name = person.name))
      .agg(person => (sum_age_or_zero = sum(person.age).orElse(10)))

    assertEquals(
      ageStats.dataFrame.collect().map(_.json).toSet,
      Set(
        """{"name":"Alice","sum_age_or_zero":30}""",
        """{"name":"David","sum_age_or_zero":10}""",
        """{"name":"Charlie","sum_age_or_zero":20}""",
        """{"name":"Bob","sum_age_or_zero":10}""",
        """{"name":"Eve","sum_age_or_zero":25}"""
      )
    )

  test("orElse chaining with aggregations"):
    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", age = 30),
        Person("Bob", age = null),
        Person("Charlie", age = 20),
        Person("David", age = null)
      )
    )

    val result = persons
      .select(person =>
        (
          name = person.name,
          age_category = when(
            lit("Unknown"),
            (person.age.orElse(0) >= 30, lit("Senior")),
            (person.age.orElse(0) >= 20, lit("Adult"))
          )
        )
      )
      .groupBy(person => (age_category = person.age_category))
      .agg(_ => (count = count(`*`)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSet,
      Set(
        """{"age_category":"Senior","count":1}""",
        """{"age_category":"Adult","count":1}""",
        """{"age_category":"Unknown","count":2}"""
      )
    )
