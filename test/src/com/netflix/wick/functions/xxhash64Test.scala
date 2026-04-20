package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*

class xxhash64Test extends FunSuite with SparkSuite:

  test("xxhash64 is deterministic and equal for equal inputs"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Alice", age = 30), Person("Bob", age = 25))
    )

    val result = persons.select(person => (name = person.name, h = person.name.xxhash64))
    val rows   = result.dataFrame.collect()

    val aliceHashes = rows.collect { case r if r.getString(0) == "Alice" => r.getLong(1) }
    val bobHash     = rows.collect { case r if r.getString(0) == "Bob" => r.getLong(1) }.head

    assertEquals(aliceHashes.distinct.length, 1, "Alice hashes should all be equal")
    assertNotEquals(aliceHashes.head, bobHash, "Alice and Bob hashes should differ")

  test("xxhash64 returns a Long"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.select(person => (h = person.name.xxhash64))
    val hash   = result.dataFrame.collect().head.getLong(0)

    // sanity check: produces a non-trivial value
    assertNotEquals(hash, 0L)

  test("xxhash64 differs across different inputs"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 40))
    )

    val hashes = persons
      .select(person => (h = person.name.xxhash64))
      .dataFrame
      .collect()
      .map(_.getLong(0))

    assertEquals(hashes.distinct.length, 3)
