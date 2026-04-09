package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite

class litTest extends FunSuite with SparkSuite:

  test("lit String produces a ScalarExpr with correct value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(_ => (status = lit("active")))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("status", StringType, nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[String]("status")).toSeq, Seq("active"))

  test("lit Int produces a ScalarExpr with correct value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(_ => (version = lit(42)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("version", IntegerType, nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Int]("version")).toSeq, Seq(42))

  test("lit Long produces a ScalarExpr with correct value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(_ => (big = lit(1234567890123L)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("big", LongType, nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("big")).toSeq, Seq(1234567890123L))

  test("lit Double produces a ScalarExpr with correct value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(_ => (ratio = lit(3.14)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("ratio", DoubleType, nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Double]("ratio")).toSeq, Seq(3.14))

  test("lit Boolean produces a ScalarExpr with correct value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(_ => (flag = lit(true)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("flag", BooleanType, nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Boolean]("flag")).toSeq, Seq(true))

  test("lit null produces a ScalarExpr of Null type"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(_ => (nothing = lit(null)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("nothing", NullType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(r => Option(r.get(0))).toSeq, Seq(None))

  test("lit can be used in arithmetic"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(p => (shifted = nullable(p.age.? + lit(10))))

    assertEquals(result.dataFrame.collect().map(_.getAs[Int]("shifted")).toSeq, Seq(40))

  test("lit produces the same value for every row"):
    val persons =
      spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35)))

    val result = persons.select(_ => (constant = lit(99)))

    assertEquals(result.dataFrame.collect().map(_.getAs[Int]("constant")).toSeq, Seq(99, 99, 99))
