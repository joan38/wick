package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite

class ExprTest extends FunSuite with SparkSuite:

  test("LinearExpr selectDynamic for nested field access"):
    case class NestedData(info: Person)
    val data = spark.createDataSeq(Seq(NestedData(Person("Alice", age = 30))))

    val result = data.select(row => (nested_name = row.info.name))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("nested_name", StringType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"nested_name":"Alice"}""")
    )

  test("LinearExpr selectDynamic for nested field in Option"):
    case class NestedData(info: Person | Null)
    val data = spark.createDataSeq(Seq(NestedData(Person("Alice", age = 30))))

    val result = data.select(row => (nested_name = nullable(row.info.?.name)))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("nested_name", StringType, nullable = true)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"nested_name":"Alice"}""")
    )
