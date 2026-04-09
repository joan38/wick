package com.netflix.wick.column

import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.Person
import munit.FunSuite

class asTest extends FunSuite with SparkSuite:

  test("convert scalar struct expr to case class"):
    val persons = spark.createDataSeq(Seq(Person("Alice", 30), Person("Bob", 25)))

    val result = persons.select(person => (person_obj = struct((name = person.name, age = person.age)).as[Person]))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"person_obj":{"name":"Alice","age":30}}""",
        """{"person_obj":{"name":"Bob","age":25}}"""
      )
    )

  test("convert linear struct expr to case class"):
    val persons = spark.createDataSeq(Seq(Person("Alice", 30), Person("Bob", 25)))

    val result = persons.select(person => (person_obj = struct((name = person.name, age = person.age)).as[Person]))

    assert(result.dataFrame.count() == 2L)

  test("mismatched types should not compile"):
    case class PersonWithAddress(name: String, age: Int | Null, address: String)

    compileErrors("""
      val persons = spark.createDataSeq(Seq(Person("Alice", 30)))
      persons.select(person => struct((name = person.name, age = person.age, address = lit("blabla"))).as[Person])
    """)

  test("mismatched field names should not compile"):
    compileErrors("""
      val persons = spark.createDataSeq(Seq(Person("Alice", 30)))
      persons.select(person => struct((fullName = person.name, age = person.age)).as[Person])
    """)
