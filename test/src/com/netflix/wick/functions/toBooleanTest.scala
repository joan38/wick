package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.model.*

class toBooleanTest extends FunSuite with SparkSuite:

  test("toBoolean converts \"true\"/\"false\" strings"):
    val data = spark.createDataSeq(Seq(StringRecord("true"), StringRecord("false"), StringRecord("true")))

    val result = data.select(row => (b = row.value.toBoolean))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("b", BooleanType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Boolean]("b")).toSeq, Seq(true, false, true))

  test("toBoolean on ScalarExpr[String] returns ScalarExpr[Boolean]"):
    val data = spark.createDataSeq(Seq(StringRecord("true")))

    val result = data.select(_ => (b = lit("true").toBoolean))

    assertEquals(result.dataFrame.schema, StructType(Array(StructField("b", BooleanType, nullable = true))))
    assertEquals(result.dataFrame.collect().map(_.getAs[Boolean]("b")).toSeq, Seq(true))

  test("toBoolean returns null for unrecognized strings"):
    val data = spark.createDataSeq(Seq(StringRecord("maybe"), StringRecord("true")))

    val result = data.select(row => (b = row.value.toBoolean))

    val values = result.dataFrame.collect().map(r => Option(r.get(0)))
    assertEquals(values.toSeq, Seq(None, Some(true)))

  test("toBoolean can be used in filter"):
    val data = spark.createDataSeq(Seq(StringRecord("true"), StringRecord("false"), StringRecord("true")))

    val result = data.filter(row => row.value.toBoolean)

    assertEquals(result.dataFrame.count(), 2L)
