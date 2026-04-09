package com.netflix.wick.operations

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class datasetTest extends FunSuite with SparkSuite:

  test("dataset returns a typed Spark Dataset"):
    val persons = Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    val dataSeq = spark.createDataSeq(persons)

    val ds = dataSeq.dataset

    assertEquals(ds.collect().toSeq, persons)

  test("dataset preserves nulls"):
    val persons = Seq(Person("Alice", age = 30), Person("Bob", age = null))
    val dataSeq = spark.createDataSeq(persons)

    val ds = dataSeq.dataset

    assertEquals(ds.collect().toSeq, persons)

  test("dataset on empty DataSeq returns empty Dataset"):
    val dataSeq = spark.createDataSeq(Seq.empty[Person])

    val ds = dataSeq.dataset

    assertEquals(ds.count(), 0L)

  test("dataset allows using Spark Dataset operations"):
    val persons = Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    val dataSeq = spark.createDataSeq(persons)

    val names = dataSeq.dataset.map(_.name)

    assertEquals(names.collect().toSeq, Seq("Alice", "Bob", "Charlie"))
