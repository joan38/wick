package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class joinTest extends FunSuite with SparkSuite:

  test("joining two DataSeqs"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 1),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 2),
        Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 1)
      )
    )

    val empDepts = employees.join(departments, _.dept_id === _.id)

    assertEquals(empDepts.dataFrame.count(), 3L)
    assertEquals(
      empDepts.dataFrame.schema,
      empDepts.dataFrame.schema,
      StructType(
        Array(
          StructField("id", IntegerType, true),
          StructField("name", StringType, true),
          StructField("dept_id", IntegerType, true),
          StructField("title_id", IntegerType, true),
          StructField("id", IntegerType, true),
          StructField("name", StringType, true)
        )
      )
    )
    assertEquals(
      empDepts.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":1,"name":"Alice","dept_id":1,"title_id":1,"id":1,"name":"Engineering"}""",
        """{"id":2,"name":"Bob","dept_id":2,"title_id":2,"id":2,"name":"Marketing"}""",
        """{"id":3,"name":"Charlie","dept_id":1,"title_id":1,"id":1,"name":"Engineering"}"""
      )
    )

  test("joining multiple DataSeqs"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 2),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 1),
        Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 2)
      )
    )
    val titles = spark.createDataSeq(
      Seq(
        Title(id = 1, name = "Ad Manager", managing = true),
        Title(id = 2, name = "Software Engineer", managing = false)
      )
    )

    val empDeptTitle = employees
      .join(departments, _.dept_id === _.id)
      .join(titles, (emp, _, title) => emp.title_id === title.id)

    assertEquals(empDeptTitle.dataFrame.count(), 3L)
    assertEquals(
      empDeptTitle.dataFrame.schema,
      StructType(
        Array(
          StructField("id", IntegerType, true),
          StructField("name", StringType, true),
          StructField("dept_id", IntegerType, true),
          StructField("title_id", IntegerType, true),
          StructField("id", IntegerType, true),
          StructField("name", StringType, true),
          StructField("id", IntegerType, true),
          StructField("name", StringType, true),
          StructField("managing", BooleanType, true)
        )
      )
    )
    assertEquals(
      empDeptTitle.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":1,"name":"Alice","dept_id":1,"title_id":2,"id":1,"name":"Engineering","id":2,"name":"Software Engineer","managing":false}""",
        """{"id":2,"name":"Bob","dept_id":2,"title_id":1,"id":2,"name":"Marketing","id":1,"name":"Ad Manager","managing":true}""",
        """{"id":3,"name":"Charlie","dept_id":1,"title_id":2,"id":1,"name":"Engineering","id":2,"name":"Software Engineer","managing":false}"""
      )
    )
