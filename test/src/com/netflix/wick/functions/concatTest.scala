package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.column.lit
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite

class concatTest extends FunSuite with SparkSuite:

  // String ++ operator

  test("string ++ operator (Expr + Expr)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person => (full = person.name ++ lit(", ") ++ person.name))

    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("full")).toSeq,
      Seq("Alice, Alice", "Bob, Bob")
    )

  test("string ++ operator (Expr + String)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person => (greeting = person.name ++ " says hello"))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("greeting", StringType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("greeting")).toSeq,
      Seq("Alice says hello", "Bob says hello")
    )

  test("string ++ operator (String + Expr)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person => (greeting = "Hello " ++ person.name))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("greeting", StringType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("greeting")).toSeq,
      Seq("Hello Alice", "Hello Bob")
    )

  test("string ++ operator (String + ScalarExpr)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (greeting = "Hello " ++ first(person.name)))

    assertEquals(
      result.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("greeting", StringType, nullable = true)
        )
      )
    )
    assertEquals(
      result.dataFrame.collect().sortBy(_.getAs[Int]("age_group")).map(_.getAs[String]("greeting")).toSeq,
      Seq("Hello Bob", "Hello Alice")
    )

  test("string ++ operator (String + String)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(_ => (label = "Hello" ++ " World"))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("label", StringType, nullable = false)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("label")).toSeq,
      Seq("Hello World", "Hello World")
    )

  // concat function

  test("concat function joins multiple string columns"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person => (label = concat(lit("Name: "), person.name, lit("!"))))

    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("label")).toSeq,
      Seq("Name: Alice!", "Name: Bob!")
    )

  // Array ++ operator

  test("array ++ operator (Expr + Expr)"):
    val data = spark.createDataSeq(
      Seq(ArraysRecord(Seq(1, 2), Seq(3, 4)), ArraysRecord(Seq(10), Seq(20, 30)))
    )

    val result = data.select(row => (merged = row.left ++ row.right))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("merged", ArrayType(IntegerType, containsNull = false), nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.getList[Int](0).toArray.toSeq).toSeq,
      Seq(Seq(1, 2, 3, 4), Seq(10, 20, 30))
    )

  // concat function for arrays

  test("concat function joins multiple array columns"):
    val data = spark.createDataSeq(
      Seq(ArraysRecord(Seq(1, 2), Seq(3, 4)))
    )

    val result = data.select(row => (merged = concat(row.left, row.right)))

    assertEquals(
      result.dataFrame.collect().map(_.getList[Int](0).toArray.toSeq).toSeq,
      Seq(Seq(1, 2, 3, 4))
    )
