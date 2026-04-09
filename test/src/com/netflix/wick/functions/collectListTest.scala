package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class collectListTest extends FunSuite with SparkSuite:

  test("collect all values into a list"):
    case class Transaction(userId: String, amount: Int)

    val transactions = spark.createDataSeq(
      Seq(
        Transaction("user1", 100),
        Transaction("user1", 200),
        Transaction("user2", 150),
        Transaction("user1", 50),
        Transaction("user2", 300)
      )
    )

    val result = transactions.agg(row => (all_amounts = collectList(row.amount)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"all_amounts":[100,200,150,50,300]}""")
    )

  test("collect values per group"):
    case class Transaction(userId: String, amount: Int)

    val transactions = spark.createDataSeq(
      Seq(
        Transaction("user1", 100),
        Transaction("user1", 200),
        Transaction("user2", 150),
        Transaction("user1", 50),
        Transaction("user2", 300)
      )
    )

    val result = transactions
      .groupBy(row => (user = row.userId))
      .agg(row => (amounts = collectList(row.amount)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"user":"user1","amounts":[100,200,50]}""",
        """{"user":"user2","amounts":[150,300]}"""
      )
    )

  test("collectList preserves duplicates"):
    case class Score(player: String, points: Int)

    val scores = spark.createDataSeq(
      Seq(
        Score("player1", 10),
        Score("player1", 10),
        Score("player1", 20),
        Score("player1", 10)
      )
    )

    val result = scores.agg(row => (all_scores = collectList(row.points)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"all_scores":[10,10,20,10]}""")
    )

  test("collectList with string values"):
    case class Event(category: String, name: String)

    val events = spark.createDataSeq(
      Seq(
        Event("sports", "soccer"),
        Event("sports", "basketball"),
        Event("music", "concert"),
        Event("sports", "tennis")
      )
    )

    val result = events
      .groupBy(row => (category = row.category))
      .agg(row => (event_names = collectList(row.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"category":"music","event_names":["concert"]}""",
        """{"category":"sports","event_names":["soccer","basketball","tennis"]}"""
      )
    )

  test("collectList with null values"):
    case class Data(group: String, value: Option[Int])

    val data = spark.createDataSeq(
      Seq(
        Data("A", Some(1)),
        Data("A", None),
        Data("A", Some(3)),
        Data("B", None),
        Data("B", Some(5))
      )
    )

    val result = data
      .groupBy(row => (group = row.group))
      .agg(row => (values = collectList(row.value)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"group":"A","values":[1,3]}""",
        """{"group":"B","values":[5]}"""
      )
    )

  test("collectList on empty dataset"):
    case class Empty(id: Int)

    val empty  = spark.createDataSeq(Seq.empty[Empty])
    val result = empty.agg(row => (ids = collectList(row.id)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"ids":[]}""")
    )

  test("collectList with single value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.agg(row => (names = collectList(row.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"names":["Alice"]}""")
    )

  test("collectList with multiple groups"):
    case class Sale(region: String, product: String, amount: Int)

    val sales = spark.createDataSeq(
      Seq(
        Sale("East", "A", 100),
        Sale("West", "B", 200),
        Sale("East", "C", 150),
        Sale("West", "A", 300),
        Sale("East", "B", 250)
      )
    )

    val result = sales
      .groupBy(row => (region = row.region))
      .agg(row => (products = collectList(row.product), amounts = collectList(row.amount)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"region":"East","products":["A","C","B"],"amounts":[100,150,250]}""",
        """{"region":"West","products":["B","A"],"amounts":[200,300]}"""
      )
    )

  test("collectList preserves insertion order"):
    case class Sequence(id: String, value: Int)

    val sequences = spark.createDataSeq(
      Seq(
        Sequence("seq1", 5),
        Sequence("seq1", 1),
        Sequence("seq1", 9),
        Sequence("seq1", 3),
        Sequence("seq1", 7)
      )
    )

    val result = sequences.agg(row => (values = collectList(row.value)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"values":[5,1,9,3,7]}""")
    )
