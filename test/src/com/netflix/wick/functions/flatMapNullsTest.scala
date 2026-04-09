package com.netflix.wick.functions

import munit.FunSuite
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class flatMapNullsTest extends FunSuite with SparkSuite:

  test("flatMapNulls transforms non-nullable elements"):
    val data = spark.createDataSeq(
      Seq(
        NumberSequence("a", Seq(1, 2, 3)),
        NumberSequence("b", Seq(4, 5, 6))
      )
    )

    val result = data.select(row =>
      (
        name = row.name,
        doubled = row.numbers.flatMapNulls(n => n * 2)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"a","doubled":[2,4,6]}""",
        """{"name":"b","doubled":[8,10,12]}"""
      )
    )

  test("flatMapNulls drops null elements"):
    case class NullableList(name: String, values: Seq[String | Null])

    val data = spark.createDataSeq(
      Seq(
        NullableList("a", Seq("1", null, "3")),
        NullableList("b", Seq(null, "5", null))
      )
    )

    assertEquals(
      data.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"a","values":["1",null,"3"]}""",
        """{"name":"b","values":[null,"5",null]}"""
      )
    )

    val result = data.select(row =>
      (
        name = row.name,
        doubled = row.values.flatMapNulls(v => v.? ++ "d")
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"a","doubled":["1d","3d"]}""",
        """{"name":"b","doubled":["5d"]}"""
      )
    )

  test("flatMapNulls extracts nullable field from struct elements, dropping nulls"):
    case class Item(label: String, value: Int | Null)
    case class Catalog(name: String, items: Seq[Item])

    val data = spark.createDataSeq(
      Seq(
        Catalog("c1", Seq(Item("a", 10), Item("b", null), Item("c", 30))),
        Catalog("c2", Seq(Item("x", null), Item("y", 20)))
      )
    )

    val result = data.select(row =>
      (
        name = row.name,
        values = row.items.flatMapNulls(item => item.value.?)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"c1","values":[10,30]}""",
        """{"name":"c2","values":[20]}"""
      )
    )

  test("flatMapNulls on empty array"):
    val data = spark.createDataSeq(Seq(NumberSequence("empty", Seq())))

    val result = data.select(row =>
      (
        name = row.name,
        doubled = row.numbers.flatMapNulls(n => n * 2)
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"empty","doubled":[]}""")
    )

  test("flatMapNulls with all-null elements produces empty array"):
    case class NullableList(name: String, values: Seq[String | Null])

    val data = spark.createDataSeq(
      Seq(NullableList("all-nulls", Seq(null, null, null)))
    )

    val result = data.select(row =>
      (
        name = row.name,
        doubled = row.values.flatMapNulls(v => v.? ++ "d")
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"all-nulls","doubled":[]}""")
    )
