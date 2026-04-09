package com.netflix.wick.operations

import munit.FunSuite
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class coalesceTest extends FunSuite with SparkSuite:

  test("coalesce reduces number of partitions"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.coalesce(1)

    assertEquals(result.dataFrame.rdd.getNumPartitions, 1)

  test("coalesce preserves all rows"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.coalesce(1)

    assertEquals(result.dataFrame.count(), 3L)
    assertEquals(
      result.dataFrame.collect().map(_.getAs[String]("name")).toSet,
      Set("Alice", "Bob", "Charlie")
    )

  test("coalesce preserves schema"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30))
    )

    val result = persons.coalesce(1)

    assertEquals(result.dataFrame.schema, persons.dataFrame.schema)

  test("coalesce on JoinedDataSeq reduces partitions"):
    val employees   = spark.createDataSeq(Seq(Employee(1, "Alice", dept_id = 10, title_id = 1)))
    val departments = spark.createDataSeq(Seq(Department(10, "Engineering")))

    val joined = employees.join(departments, (emp, dept) => emp.dept_id === dept.id)

    val result = joined.coalesce(1)

    assertEquals(result.dataFrame.rdd.getNumPartitions, 1)
