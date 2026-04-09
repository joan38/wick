package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class maxTest extends FunSuite with SparkSuite:

  test("finding maximum age in grouped data"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Anais", age = 25))
    )

    val aggregated = persons.groupBy(person => (age_group = person.age)).agg(person => (max_name = max(person.name)))
    assertEquals(
      aggregated.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"max_name":"Bob"}""",
        """{"age_group":30,"max_name":"Alice"}"""
      )
    )

  test("finding maximum age across all persons"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (oldest_age = max(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"oldest_age":35}""")
    )

  test("finding maximum with null values"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 25))
    )

    val result = personsWithNulls.agg(person => (max_age = max(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"max_age":30}""")
    )

  test("finding maximum in empty dataset"):
    val emptyPersons = spark.createDataSeq(Seq.empty[Person])
    val result       = emptyPersons.agg(person => (max_age = max(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"max_age":null}""")
    )

  test("finding maximum string values"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (max_name = max(person.name)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"max_name":"Charlie"}""")
    )

  test("using * in max as max(*) should not compile"):
    compileErrors("""
      import com.netflix.wick.functions.`*`

      val persons =
        spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35)))

      persons.groupBy(person => (age_group = person.age)).agg(_ => (max_age = max(`*`)))
    """)

  test("finding maximum value in array"):
    case class GradeBook(student: String, grades: Seq[Int])

    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78)),
        GradeBook("Bob", Seq(92, 88, 95)),
        GradeBook("Charlie", Seq(70, 85, 82))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        highest_grade = row.grades.max
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","highest_grade":90}""",
        """{"student":"Bob","highest_grade":95}""",
        """{"student":"Charlie","highest_grade":85}"""
      )
    )

  test("finding maximum in array with scalar expressions"):
    case class GradeBook(student: String, grades: Seq[Int])

    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78)),
        GradeBook("Bob", Seq(92, 88, 95))
      )
    )

    val result = gradeBooks
      .agg(row =>
        (
          combined_grades = array(sum(row.grades.max), min(row.grades.max))
        )
      )
      .select(row =>
        (
          overall_max = row.combined_grades.max
        )
      )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"overall_max":185}""")
    )

  test("finding maximum in empty array"):
    case class GradeBook(student: String, grades: Seq[Int])

    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq()),
        GradeBook("Bob", Seq(85, 90))
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        highest_grade = row.grades.max
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","highest_grade":null}""",
        """{"student":"Bob","highest_grade":90}"""
      )
    )

  test("finding maximum in array with null array"):
    case class NullableGradeBook(student: String, grades: Seq[Int] | Null)

    val gradeBooks = spark.createDataSeq(
      Seq(
        NullableGradeBook("Alice", Seq(85, 90, 78)),
        NullableGradeBook("Bob", null)
      )
    )

    val result = gradeBooks.select(row =>
      (
        student = row.student,
        highest_grade = row.grades.orElse(array[Int]()).max
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","highest_grade":90}""",
        """{"student":"Bob","highest_grade":null}"""
      )
    )
