package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*
import java.math.BigDecimal

class toDecimalTest extends FunSuite with SparkSuite:

  test("toDecimal on Expr[BigDecimal] casts to the given precision and scale"):
    case class DecimalRecord(amount: BigDecimal)

    val data = spark.createDataSeq(
      Seq(
        DecimalRecord(BigDecimal("1.23")),
        DecimalRecord(BigDecimal("4.5678")),
        DecimalRecord(BigDecimal("-9.9"))
      )
    )

    val result = data.select(row => (n = row.amount.toDecimal(10, 2)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DecimalType(10, 2), nullable = true))))
    assertEquals(
      result.dataFrame.collect().map(_.getAs[BigDecimal]("n")).toSeq,
      Seq(BigDecimal("1.23"), BigDecimal("4.57"), BigDecimal("-9.90"))
    )

  test("toDecimal on ScalarExpr[BigDecimal] returns ScalarExpr[BigDecimal]"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(BigDecimal("3.14159")).toDecimal(6, 2)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DecimalType(6, 2), nullable = false))))
    assertEquals(result.dataFrame.collect().map(_.getAs[BigDecimal]("n")).toSeq, Seq(BigDecimal("3.14")))

  test("toDecimal on Expr[Double] casts to the given precision and scale"):
    val data = spark.createDataSeq(Seq(Transaction("t1", 3.14), Transaction("t2", -2.5), Transaction("t3", 100.0)))

    val result = data.select(row => (n = row.amount.toDecimal(8, 3)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DecimalType(8, 3), nullable = true))))
    assertEquals(
      result.dataFrame.collect().map(_.getAs[BigDecimal]("n")).toSeq,
      Seq(BigDecimal("3.140"), BigDecimal("-2.500"), BigDecimal("100.000"))
    )

  test("toDecimal on ScalarExpr[Double] returns ScalarExpr[BigDecimal]"):
    val data = spark.createDataSeq(Seq(StringRecord("x")))

    val result = data.select(_ => (n = lit(2.71828).toDecimal(5, 2)))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("n", DecimalType(5, 2), nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[BigDecimal]("n")).toSeq, Seq(BigDecimal("2.72")))
