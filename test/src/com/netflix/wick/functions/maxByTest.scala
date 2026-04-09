package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class maxByTest extends FunSuite with SparkSuite:

  test("finding name of students with maximum score2 in grouped data"):
    val students = spark.createDataSeq(
      Seq(
        Student("Alice", score1 = 30, score2 = 1, score3 = 0),
        Student("Bob", score1 = 25, score2 = 2, score3 = 0),
        Student("Charlie", score1 = 35, score2 = 3, score3 = 0),
        Student("Diana", score1 = 25, score2 = 4, score3 = 0)
      )
    )

    val aggregated = students
      .groupBy(student => (score1_group = student.score1))
      .agg(student => (name_with_max_score2 = maxBy(student.name, student.score2)))
    assertEquals(
      aggregated.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"score1_group":25,"name_with_max_score2":"Diana"}""",
        """{"score1_group":30,"name_with_max_score2":"Alice"}""",
        """{"score1_group":35,"name_with_max_score2":"Charlie"}"""
      )
    )

  test("finding name of oldest person across all persons"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (oldest_person = maxBy(person.name, person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"oldest_person":"Charlie"}""")
    )

  test("finding maxBy with null values in by expression"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 25))
    )

    val result = personsWithNulls.agg(person => (oldest_person = maxBy(person.name, person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"oldest_person":"Alice"}""")
    )

  test("finding maxBy in empty dataset"):
    val emptyPersons = spark.createDataSeq(Seq.empty[Person])
    val result       = emptyPersons.agg(person => (oldest_person = maxBy(person.name, person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"oldest_person":null}""")
    )

  test("finding maxBy with integer as value expression"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (max_age = maxBy(person.age, person.name)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"max_age":35}""")
    )

  test("using * in maxBy should not compile"):
    compileErrors("""
      import com.netflix.wick.functions.`*`

      val persons =
        spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35)))

      persons.groupBy(person => (age_group = person.age)).agg(_ => (result = maxBy(`*`, person.age)))
    """)

  test("finding maxBy with string ordering"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Zoe", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (age_with_max_name = maxBy(person.age, person.name)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"age_with_max_name":25}""")
    )

  test("finding maxBy with multiple groups"):
    case class Student(name: String, score1: Int, score2: Int, score3: Int)

    val students = spark.createDataSeq(
      Seq(
        Student("Alice", 85, 90, 78),
        Student("Bob", 92, 88, 95),
        Student("Charlie", 70, 85, 82)
      )
    )

    val result = students.agg(student =>
      (
        best_student_score1 = maxBy(student.name, student.score1),
        best_student_score2 = maxBy(student.name, student.score2),
        best_student_score3 = maxBy(student.name, student.score3)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"best_student_score1":"Bob","best_student_score2":"Alice","best_student_score3":"Bob"}""")
    )

  test("finding maxBy with ties returns arbitrary value"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 30), Person("Charlie", age = 25))
    )

    val result      = persons.agg(person => (person_with_max_age = maxBy(person.name, person.age)))
    val resultValue = result.dataFrame.collect().map(_.json).toSeq.head
    assert(
      resultValue == """{"person_with_max_age":"Alice"}""" || resultValue == """{"person_with_max_age":"Bob"}"""
    )
