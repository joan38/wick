package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class orderByTest extends FunSuite with SparkSuite:

  test("sorting by multiple columns"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.orderBy(person => (person.age, person.name))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Bob","age":25}""",
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("sorting by a single column"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.orderBy(_.age)
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Bob","age":25}""",
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("sorting by not a Col should fail compilation"):
    compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.orderBy(person => 1)
    """)

  test("sorting by not Col should fail compilation"):
    compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.orderBy(_ => (1, 2))
    """)

  test("sorting by not orderable expressions should fail compilation"):
    compileErrors("""
      case class ComplexData(name: String, metadata: Map[String, Int])

      val complex = spark.createDataSeq(Seq(
        ComplexData("Alice", Map("score" -> 100)),
        ComplexData("Bob", Map("score" -> 85))
      ))

      // This will NOT compile - Map types are not orderable
      complex.orderBy(_.metadata) // Compilation error!
    """)
