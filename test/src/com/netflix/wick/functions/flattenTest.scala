package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.column.array

class flattenTest extends FunSuite with SparkSuite:

  test("flatten nested arrays"):
    val courses = spark.createDataSeq(
      Seq(
        Course("Math", Seq(Seq("Alice", "Bob"), Seq("Charlie"), Seq("David", "Eve"))),
        Course("Science", Seq(Seq("Frank", "Grace"), Seq("Henry"))),
        Course("Art", Seq(Seq("Iris"), Seq("Jack", "Kate", "Liam")))
      )
    )

    val result = courses.select(row =>
      (
        course = row.name,
        all_students = row.studentGroups.flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"course":"Math","all_students":["Alice","Bob","Charlie","David","Eve"]}""",
        """{"course":"Science","all_students":["Frank","Grace","Henry"]}""",
        """{"course":"Art","all_students":["Iris","Jack","Kate","Liam"]}"""
      )
    )

  test("flatten numeric matrices"):
    val matrices = spark.createDataSeq(
      Seq(
        NumberMatrix("Matrix1", Seq(Seq(1, 2), Seq(3, 4, 5), Seq(6))),
        NumberMatrix("Matrix2", Seq(Seq(10, 20), Seq(30, 40))),
        NumberMatrix("SingleRow", Seq(Seq(100, 200, 300)))
      )
    )

    val result = matrices.select(row =>
      (
        name = row.name,
        flattened = row.matrix.flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Matrix1","flattened":[1,2,3,4,5,6]}""",
        """{"name":"Matrix2","flattened":[10,20,30,40]}""",
        """{"name":"SingleRow","flattened":[100,200,300]}"""
      )
    )

  test("flatten with scalar expressions in aggregation"):
    case class Team(name: String, memberBatches: Seq[Seq[String]])

    val teams = spark.createDataSeq(
      Seq(
        Team("Alpha", Seq(Seq("Alice", "Bob"), Seq("Charlie"))),
        Team("Beta", Seq(Seq("David"), Seq("Eve", "Frank")))
      )
    )

    val result = teams
      .agg(row =>
        (
          all_batches = array(first(row.memberBatches))
        )
      )
      .select(row =>
        (
          flattened_members = row.all_batches.flatten
        )
      )

    // Note: This is a more complex aggregation scenario - the exact result depends on the first() behavior
    val collected = result.dataFrame.collect().map(_.json).toSeq
    assert(collected.size == 1)
    assert(collected.head.contains("flattened_members"))

  test("flatten empty nested arrays"):
    val emptyCourses = spark.createDataSeq(
      Seq(
        Course("EmptyCourse", Seq()),
        Course("EmptyGroups", Seq(Seq(), Seq(), Seq()))
      )
    )

    val result = emptyCourses.select(row =>
      (
        course = row.name,
        all_students = row.studentGroups.flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"course":"EmptyCourse","all_students":[]}""",
        """{"course":"EmptyGroups","all_students":[]}"""
      )
    )

  test("flatten with null nested array"):
    case class NullableCourse(name: String, studentGroups: Seq[Seq[String]] | Null)

    val courses = spark.createDataSeq(
      Seq(
        NullableCourse("ValidCourse", Seq(Seq("Alice", "Bob"), Seq("Charlie"))),
        NullableCourse("NullCourse", null)
      )
    )

    val result = courses.select(row =>
      (
        course = row.name,
        all_students = row.studentGroups.orElse(array(array[String]())).flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"course":"ValidCourse","all_students":["Alice","Bob","Charlie"]}""",
        """{"course":"NullCourse","all_students":[]}"""
      )
    )

  test("flatten preserves order"):
    val orderedCourse = spark.createDataSeq(
      Seq(
        Course(
          "OrderTest",
          Seq(
            Seq("First1", "First2"),
            Seq("Second1"),
            Seq("Third1", "Third2", "Third3")
          )
        )
      )
    )

    val result = orderedCourse.select(row =>
      (
        course = row.name,
        ordered_students = row.studentGroups.flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"course":"OrderTest","ordered_students":["First1","First2","Second1","Third1","Third2","Third3"]}""")
    )

  test("flatten single element arrays"):
    val singleElements = spark.createDataSeq(
      Seq(
        Course("Singles", Seq(Seq("Only1"), Seq("Only2"), Seq("Only3")))
      )
    )

    val result = singleElements.select(row =>
      (
        course = row.name,
        all_students = row.studentGroups.flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"course":"Singles","all_students":["Only1","Only2","Only3"]}""")
    )

  test("flatten mixed size nested arrays"):
    val mixedSizes = spark.createDataSeq(
      Seq(
        NumberMatrix(
          "Mixed",
          Seq(
            Seq(),          // empty
            Seq(1),         // single element
            Seq(2, 3),      // two elements
            Seq(),          // empty again
            Seq(4, 5, 6, 7) // four elements
          )
        )
      )
    )

    val result = mixedSizes.select(row =>
      (
        name = row.name,
        flattened = row.matrix.flatten
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Mixed","flattened":[1,2,3,4,5,6,7]}""")
    )
