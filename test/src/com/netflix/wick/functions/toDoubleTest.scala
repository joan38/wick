package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*
import java.math.BigDecimal

class toDoubleTest extends FunSuite with SparkSuite:

  test("toDouble converts string column to Double"):
    val data = spark.createDataSeq(Seq(StringRecord("3.14"), StringRecord("2.718"), StringRecord("-0.5")))

    val result = data.select(row => (n = row.value.toDouble))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DoubleType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Double]("n")).toSeq, Seq(3.14, 2.718, -0.5))

  test("toDouble on ScalarExpr[String] returns ScalarExpr[Double]"):
    val data = spark.createDataSeq(Seq(StringRecord("1.5")))

    val result = data.select(_ => (n = lit("1.5").toDouble))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DoubleType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Double]("n")).toSeq, Seq(1.5))

  test("toDouble returns null for non-numeric strings"):
    val data = spark.createDataSeq(Seq(StringRecord("hello"), StringRecord("1.23")))

    val result = data.select(row => (n = row.value.toDouble))

    val values = result.dataFrame.collect().map(r => Option(r.get(0)))
    assertEquals(values.toSeq, Seq(None, Some(1.23)))

  test("toDouble converts integer strings"):
    val data = spark.createDataSeq(Seq(StringRecord("42"), StringRecord("0")))

    val result = data.select(row => (n = row.value.toDouble))

    assertEquals(result.dataFrame.collect().map(_.getAs[Double]("n")).toSeq, Seq(42.0, 0.0))

  test("toDouble converts BigDecimal column to Double"):
    case class DecimalRecord(amount: BigDecimal)

    val data = spark.createDataSeq(
      Seq(
        DecimalRecord(BigDecimal("3.14")),
        DecimalRecord(BigDecimal("2.718")),
        DecimalRecord(BigDecimal("-0.5"))
      )
    )

    val result = data.select(row => (n = row.amount.toDouble))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DoubleType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Double]("n")).toSeq, Seq(3.14, 2.718, -0.5))

  test("toDouble on ScalarExpr[BigDecimal] returns ScalarExpr[Double]"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(BigDecimal("1.5")).toDouble))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DoubleType, nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Double]("n")).toSeq, Seq(1.5))
