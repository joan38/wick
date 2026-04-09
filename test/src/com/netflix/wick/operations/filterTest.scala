package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*

class filterTest extends FunSuite with SparkSuite:

  test("filtering with numeric comparison operators"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val adults = persons.filter(_.age.orElse(0) >= 18)
    assertEquals(adults.dataFrame.count(), 2L)
    assertEquals(
      adults.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

    val youngAdults = persons.filter(_.age.orElse(0) < 25)
    assertEquals(youngAdults.dataFrame.count(), 1L)
    assertEquals(
      youngAdults.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Bob","age":15}""")
    )

    val exactAge = persons.filter(_.age === 30)
    assertEquals(exactAge.dataFrame.count(), 1L)
    assertEquals(
      exactAge.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","age":30}""")
    )

  test("filtering with string equality operators"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val alice = persons.filter(_.name === "Alice")
    assertEquals(alice.dataFrame.count(), 1L)
    assertEquals(
      alice.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","age":30}""")
    )

    val notBob = persons.filter(_.name !== "Bob")
    assertEquals(notBob.dataFrame.count(), 2L)
    assertEquals(
      notBob.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("filtering with logical operators"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    // AND operator
    val middleAged = persons.filter(person => person.age.orElse(0) > 25 && person.age.orElse(0) < 35)
    assertEquals(middleAged.dataFrame.count(), 1L)
    assertEquals(
      middleAged.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","age":30}""")
    )

    // OR operator
    val youngOrOld = persons.filter(person => person.age.orElse(0) < 20 || person.age.orElse(0) > 30)
    assertEquals(youngOrOld.dataFrame.count(), 2L)
    assertEquals(
      youngOrOld.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Bob","age":15}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("filtering with negation"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val notBob = persons.filter(person => !(person.name === "Bob"))
    assertEquals(notBob.dataFrame.count(), 2L)
    assertEquals(
      notBob.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

    val notAdult = persons.filter(person => !(person.age.orElse(0) >= 18))
    assertEquals(notAdult.dataFrame.count(), 1L)
    assertEquals(
      notAdult.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Bob","age":15}""")
    )

  test("filtering empty DataSeq"):
    val emptyPersons = spark.createDataSeq(Seq.empty[Person])
    val filtered     = emptyPersons.filter(_.age.orElse(0) > 18)
    assertEquals(filtered.dataFrame.count(), 0L)

  test("filtering with no matches"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )
    val noMatch = persons.filter(_.age.orElse(0) > 100)
    assertEquals(noMatch.dataFrame.count(), 0L)

  test("filtering all matches"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )
    val allMatch = persons.filter(_.age.orElse(0) > 0)
    assertEquals(allMatch.dataFrame.count(), 3L)

  test("chaining filters"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val chainedFilters = persons
      .filter(_.age.orElse(0) >= 18)
      .filter(_.name !== "Charlie")

    assertEquals(chainedFilters.dataFrame.count(), 1L)
    assertEquals(
      chainedFilters.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","age":30}""")
    )

  test("filtering JoinedDataSeq"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 1),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 2),
        Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 1)
      )
    )

    val joined               = employees.join(departments, (emp, dept) => emp.dept_id === dept.id)
    val engineeringEmployees = joined.filter((_, dept) => dept.name === "Engineering")

    assertEquals(engineeringEmployees.dataFrame.count(), 2L)
    assertEquals(
      engineeringEmployees.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"id":1,"name":"Alice","dept_id":1,"title_id":1,"id":1,"name":"Engineering"}""",
        """{"id":3,"name":"Charlie","dept_id":1,"title_id":1,"id":1,"name":"Engineering"}"""
      ).sorted
    )

  test("filtering JoinedDataSeq with complex conditions"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 1),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 2),
        Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 1)
      )
    )

    val joined            = employees.join(departments, (emp, dept) => emp.dept_id === dept.id)
    val filteredEmployees = joined.filter((emp, dept) => dept.name === "Engineering" && emp.id > 1)

    assertEquals(filteredEmployees.dataFrame.count(), 1L)
    assertEquals(
      filteredEmployees.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"id":3,"name":"Charlie","dept_id":1,"title_id":1,"id":1,"name":"Engineering"}""")
    )
