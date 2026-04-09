package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class absTest extends FunSuite with SparkSuite:

  test("absolute value of positive numbers"):
    val transactions = spark.createDataSeq(
      Seq(
        Transaction("t1", 100.0),
        Transaction("t2", 50.0),
        Transaction("t3", 25.5)
      )
    )

    val result = transactions.select(row =>
      (
        id = row.id,
        abs_amount = abs(row.amount)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":"t1","abs_amount":100.0}""",
        """{"id":"t2","abs_amount":50.0}""",
        """{"id":"t3","abs_amount":25.5}"""
      )
    )

  test("absolute value of negative numbers"):
    val transactions = spark.createDataSeq(
      Seq(
        Transaction("t1", -100.0),
        Transaction("t2", -50.0),
        Transaction("t3", -25.5)
      )
    )

    val result = transactions.select(row =>
      (
        id = row.id,
        abs_amount = abs(row.amount)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":"t1","abs_amount":100.0}""",
        """{"id":"t2","abs_amount":50.0}""",
        """{"id":"t3","abs_amount":25.5}"""
      )
    )

  test("absolute value of mixed positive and negative numbers"):
    val transactions = spark.createDataSeq(
      Seq(
        Transaction("t1", -100.0),
        Transaction("t2", 50.0),
        Transaction("t3", -25.5),
        Transaction("t4", 0.0)
      )
    )

    val result = transactions.select(row =>
      (
        id = row.id,
        abs_amount = abs(row.amount)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":"t1","abs_amount":100.0}""",
        """{"id":"t2","abs_amount":50.0}""",
        """{"id":"t3","abs_amount":25.5}""",
        """{"id":"t4","abs_amount":0.0}"""
      )
    )

  test("absolute value with integer values"):
    val temperatures = spark.createDataSeq(
      Seq(
        Person("Arctic", -30),
        Person("Desert", 45),
        Person("Freezer", -18),
        Person("Room", 22)
      )
    )

    val result = temperatures.select(row =>
      (
        name = row.name,
        abs_age = abs(row.age)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Arctic","abs_age":30}""",
        """{"name":"Desert","abs_age":45}""",
        """{"name":"Freezer","abs_age":18}""",
        """{"name":"Room","abs_age":22}"""
      )
    )

  test("absolute value in aggregation with scalar expressions"):
    val transactions = spark.createDataSeq(
      Seq(
        Transaction("t1", -100.0),
        Transaction("t2", 50.0),
        Transaction("t3", -25.5),
        Transaction("t4", 75.0)
      )
    )

    val result = transactions.agg(row =>
      (
        net_amount = sum(row.amount),
        abs_net_amount = abs(sum(row.amount)),
        total_abs_amount = sum(abs(row.amount))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"net_amount":-0.5,"abs_net_amount":0.5,"total_abs_amount":250.5}""")
    )

  test("absolute value with null values"):
    case class NullableTransaction(id: String, amount: Double | Null)

    val transactions = spark.createDataSeq(
      Seq(
        NullableTransaction("t1", -100.0),
        NullableTransaction("t2", null),
        NullableTransaction("t3", 25.5)
      )
    )

    val result = transactions.select(row =>
      (
        id = row.id,
        abs_amount = abs(row.amount)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":"t1","abs_amount":100.0}""",
        """{"id":"t2","abs_amount":null}""",
        """{"id":"t3","abs_amount":25.5}"""
      )
    )

  test("absolute value in grouped aggregation"):
    val transactions = spark.createDataSeq(
      Seq(
        Transaction("credit", 100.0),
        Transaction("debit", -50.0),
        Transaction("credit", 75.0),
        Transaction("debit", -30.0)
      )
    )

    val result = transactions
      .groupBy(row => (`type` = row.id))
      .agg(row =>
        (
          total = sum(row.amount),
          abs_total = abs(sum(row.amount)),
          sum_of_abs = sum(abs(row.amount))
        )
      )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSet,
      Set(
        """{"type":"credit","total":175.0,"abs_total":175.0,"sum_of_abs":175.0}""",
        """{"type":"debit","total":-80.0,"abs_total":80.0,"sum_of_abs":80.0}"""
      )
    )

  test("absolute value with zero"):
    val transactions = spark.createDataSeq(
      Seq(
        Transaction("zero1", 0.0),
        Transaction("zero2", -0.0)
      )
    )

    val result = transactions.select(row =>
      (
        id = row.id,
        abs_amount = abs(row.amount)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"id":"zero1","abs_amount":0.0}""",
        """{"id":"zero2","abs_amount":0.0}"""
      )
    )

  test("absolute value in empty dataset"):
    val emptyTransactions = spark.createDataSeq(Seq.empty[Transaction])
    val result            = emptyTransactions.agg(row => (abs_sum = abs(sum(row.amount))))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"abs_sum":null}""")
    )
