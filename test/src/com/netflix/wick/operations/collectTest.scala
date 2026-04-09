package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class collectTest extends FunSuite with SparkSuite:

  test("collect case classes"):
    val persons = Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    val dataSeq = spark.createDataSeq(persons)

    val collected = dataSeq.collect()
    assertEquals(collected.length, 3)
    assertEquals(collected.toSeq, persons)

  test("collect named tuples"):
    val persons =
      Seq((name = "Alice", age = 30), (name = "Bob", age = 15), (name = "Charlie", age = 35))
    val dataSeq = spark.createDataSeq(persons)

    val collected = dataSeq.collect()
    assertEquals(collected.length, 3)
    assertEquals(collected.toSeq, persons)
