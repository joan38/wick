package com.netflix.wick.operations

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*

class countTest extends FunSuite with SparkSuite:

  test("counting rows in a DataSeq"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )
    assertEquals(persons.count(), 3L)

  test("counting rows in an empty DataSeq"):
    val emptyPersons = spark.createDataSeq(Seq.empty[Person])
    assertEquals(emptyPersons.count(), 0L)

  test("counting rows in a single-element DataSeq"):
    val singlePerson = spark.createDataSeq(Seq(Person("Alice", age = 30)))
    assertEquals(singlePerson.count(), 1L)

  test("counting rows in a filtered DataSeq"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )
    val adults = persons.filter(_.age.orElse(0) >= 18)
    assertEquals(adults.count(), 2L)

  test("counting rows in a JoinedDataSeq"):
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

    val empDepts = employees.join(departments, _.dept_id === _.id)
    assertEquals(empDepts.count(), 3L)

  test("counting rows in a multiple-joined DataSeq"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 2),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 1)
      )
    )
    val titles = spark.createDataSeq(
      Seq(
        Title(id = 1, name = "Ad Manager", managing = true),
        Title(id = 2, name = "Software Engineer", managing = false)
      )
    )

    val empDeptTitle = employees
      .join(departments, _.dept_id === _.id)
      .join(titles, (emp, _, title) => emp.title_id === title.id)

    assertEquals(empDeptTitle.count(), 2L)

  test("counting rows in a joined DataSeq with no matches"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 99, title_id = 1) // dept_id 99 doesn't exist
      )
    )

    val empDepts = employees.join(departments, _.dept_id === _.id)
    assertEquals(empDepts.count(), 0L)
