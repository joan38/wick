package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class minTest extends FunSuite with SparkSuite:

  test("finding minimum age in grouped data"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Anais", age = 25))
    )

    val aggregated = persons.groupBy(person => (age_group = person.age)).agg(person => (min_name = min(person.name)))
    assertEquals(
      aggregated.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"min_name":"Anais"}""",
        """{"age_group":30,"min_name":"Alice"}"""
      )
    )

  test("finding minimum age across all persons"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.agg(person => (youngest_age = min(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"youngest_age":25}""")
    )

  test("finding minimum with null values"):
    val personsWithNulls = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 25))
    )

    val result = personsWithNulls.agg(person => (min_age = min(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"min_age":25}""")
    )

  test("finding minimum in empty dataset"):
    val emptyPersons = spark.createDataSeq(Seq.empty[Person])
    val result       = emptyPersons.agg(person => (min_age = min(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"min_age":null}""")
    )

  test("finding minimum string values"):
    val persons = spark.createDataSeq(
      Seq(Person("Charlie", age = 30), Person("Alice", age = 25), Person("Bob", age = 35))
    )

    val result = persons.agg(person => (min_name = min(person.name)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"min_name":"Alice"}""")
    )

  test("using * in min as min(*) should not compile"):
    compileErrors("""
      import com.netflix.wick.functions.`*`

      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.groupBy(person => (age_group = person.age)).agg(_ => (min_age = min(`*`)))
    """)

  test("finding minimum value in array"):
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
        lowest_grade = row.grades.min
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","lowest_grade":78}""",
        """{"student":"Bob","lowest_grade":88}""",
        """{"student":"Charlie","lowest_grade":70}"""
      )
    )

  test("finding minimum in array with scalar expressions"):
    case class GradeBook(student: String, grades: Seq[Int])

    val gradeBooks = spark.createDataSeq(
      Seq(
        GradeBook("Alice", Seq(85, 90, 78)),
        GradeBook("Bob", Seq(92, 88, 95))
      )
    )

    val result = gradeBooks
      .agg(row => (combined_grades = array(sum(row.grades.min), max(row.grades.min))))
      .select(row => (overall_min = row.combined_grades.min))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"overall_min":88}""")
    )

  test("finding minimum in empty array"):
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
        lowest_grade = row.grades.min
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","lowest_grade":null}""",
        """{"student":"Bob","lowest_grade":85}"""
      )
    )

  test("finding minimum in array with null array"):
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
        lowest_grade = row.grades.orElse(array[Int]()).min
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"student":"Alice","lowest_grade":78}""",
        """{"student":"Bob","lowest_grade":null}"""
      )
    )
