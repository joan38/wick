package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*
import java.math.BigDecimal

class toLongTest extends FunSuite with SparkSuite:

  test("toLong converts string column to Long"):
    val data = spark.createDataSeq(Seq(StringRecord("1234567890123"), StringRecord("9876543210"), StringRecord("-42")))

    val result = data.select(row => (n = row.value.toLong))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", LongType, nullable = true))))
    assertEquals(
      result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq,
      Seq(1234567890123L, 9876543210L, -42L)
    )

  test("toLong on ScalarExpr[String] returns ScalarExpr[Long]"):
    val data = spark.createDataSeq(Seq(StringRecord("999")))

    val result = data.select(_ => (n = lit("999").toLong))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", LongType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(999L))

  test("toLong returns null for non-numeric strings"):
    val data = spark.createDataSeq(Seq(StringRecord("hello"), StringRecord("123")))

    val result = data.select(row => (n = row.value.toLong))

    val values = result.dataFrame.collect().map(r => Option(r.get(0)))
    assertEquals(values.toSeq, Seq(None, Some(123L)))

  test("toLong handles values larger than Int.MaxValue"):
    val large = (Int.MaxValue.toLong + 1).toString
    val data  = spark.createDataSeq(Seq(StringRecord(large)))

    val result = data.select(row => (n = row.value.toLong))

    assertEquals(
      result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq,
      Seq(Int.MaxValue.toLong + 1)
    )

  test("toLong truncates Double column to Long"):
    val data = spark.createDataSeq(Seq(Transaction("a", 3.7), Transaction("b", -1.9), Transaction("c", 100.0)))

    val result = data.select(row => (n = row.amount.toLong))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", LongType, nullable = true))))
    assertEquals(
      result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq,
      Seq(3L, -1L, 100L)
    )

  test("toLong on ScalarExpr[Double] via lit truncates to Long"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(42.9).toLong))

    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(42L))

  test("toLong converts Int column to Long"):
    val data = spark.createDataSeq(Seq(Department(1, "Engineering"), Department(2, "Marketing")))

    val result = data.select(row => (n = row.id.toLong))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", LongType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(1L, 2L))

  test("toLong on ScalarExpr[Int] via lit converts to Long"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(5).toLong))

    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(5L))

  test("toLong on ScalarExpr[Long] via lit is identity"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(10L).toLong))

    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(10L))

  test("toLong on ScalarExpr[Float] via lit truncates to Long"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(2.5f).toLong))

    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(2L))

  test("toLong on ScalarExpr[BigDecimal] via lit truncates to Long"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(BigDecimal("42.9")).toLong))

    assertEquals(result.dataFrame.collect().map(_.getAs[Long]("n")).toSeq, Seq(42L))
