package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class toIntTest extends FunSuite with SparkSuite:

  test("toInt converts string column to Int"):
    val data = spark.createDataSeq(Seq(StringRecord("42"), StringRecord("100"), StringRecord("-5")))

    val result = data.select(row => (n = row.value.toInt))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", IntegerType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Int]("n")).toSeq, Seq(42, 100, -5))

  test("toInt on ScalarExpr[String] returns ScalarExpr[Int]"):
    val data = spark.createDataSeq(Seq(StringRecord("7")))

    val result = data.select(_ => (n = lit("7").toInt))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", IntegerType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Int]("n")).toSeq, Seq(7))

  test("toInt returns null for non-numeric strings"):
    val data = spark.createDataSeq(Seq(StringRecord("hello"), StringRecord("42")))

    val result = data.select(row => (n = row.value.toInt))

    val values = result.dataFrame.collect().map(r => Option(r.get(0)))
    assertEquals(values.toSeq, Seq(None, Some(42)))

  test("toInt can be used in arithmetic"):
    val data = spark.createDataSeq(Seq(StringRecord("10"), StringRecord("20")))

    val result = data.select(row => (n = row.value.toInt + 5))

    assertEquals(result.dataFrame.collect().map(_.getAs[Int]("n")).toSeq, Seq(15, 25))
