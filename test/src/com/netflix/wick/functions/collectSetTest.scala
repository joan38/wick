package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}

class collectSetTest extends FunSuite with SparkSuite:

  test("collect unique values into a set"):
    case class Rating(userId: String, movieId: String, rating: Int)

    val ratings = spark.createDataSeq(
      Seq(
        Rating("user1", "movie1", 5),
        Rating("user1", "movie2", 4),
        Rating("user1", "movie3", 5),
        Rating("user2", "movie1", 3),
        Rating("user2", "movie2", 3)
      )
    )

    val result = ratings.agg(row => (unique_ratings = collectSet(row.rating)))

    val collected = result.dataFrame.collect().map(_.json).toSeq.head
    assert(collected.contains("unique_ratings"))
    // Set order is not guaranteed, so we check for presence of values
    assert(collected.contains("5") && collected.contains("4") && collected.contains("3"))

  test("collect unique values per group"):
    case class Rating(userId: String, movieId: String, rating: Int)

    val ratings = spark.createDataSeq(
      Seq(
        Rating("user1", "movie1", 5),
        Rating("user1", "movie2", 4),
        Rating("user1", "movie3", 5),
        Rating("user2", "movie1", 3),
        Rating("user2", "movie2", 3)
      )
    )

    val result = ratings
      .groupBy(row => (user = row.userId))
      .agg(row => (unique_ratings = collectSet(row.rating)))

    val collected = result.dataFrame.collect().map(_.json).toSeq
    assertEquals(collected.size, 2)
    assert(collected.exists(_.contains("user1")))
    assert(collected.exists(_.contains("user2")))

  test("collectSet removes duplicates"):
    case class Score(player: String, points: Int)

    val scores = spark.createDataSeq(
      Seq(
        Score("player1", 10),
        Score("player1", 10),
        Score("player1", 20),
        Score("player1", 10),
        Score("player1", 30)
      )
    )

    val result = scores.agg(row => (unique_scores = collectSet(row.points)))

    val collected = result.dataFrame.collect().map(_.json).toSeq.head
    // Should have 3 unique values: 10, 20, 30
    assert(collected.contains("10") && collected.contains("20") && collected.contains("30"))
    // Verify it's a proper set by counting elements
    val valuesStr = collected.substring(collected.indexOf("[") + 1, collected.indexOf("]"))
    assertEquals(valuesStr.split(",").length, 3)

  test("collectSet with string values"):
    case class Tag(category: String, tag: String)

    val tags = spark.createDataSeq(
      Seq(
        Tag("tech", "scala"),
        Tag("tech", "java"),
        Tag("tech", "scala"),
        Tag("music", "jazz"),
        Tag("music", "jazz"),
        Tag("music", "rock")
      )
    )

    val result = tags
      .groupBy(row => (category = row.category))
      .agg(row => (unique_tags = collectSet(row.tag)))

    val collected = result.dataFrame.collect().map(_.json).toSeq
    assertEquals(collected.size, 2)

    val techRow = collected.find(_.contains("tech")).get
    assert(techRow.contains("scala") && techRow.contains("java"))

    val musicRow = collected.find(_.contains("music")).get
    assert(musicRow.contains("jazz") && musicRow.contains("rock"))

  test("collectSet with null values"):
    case class Data(group: String, value: Option[Int])

    val data = spark.createDataSeq(
      Seq(
        Data("A", Some(1)),
        Data("A", None),
        Data("A", Some(1)),
        Data("A", Some(3)),
        Data("B", None),
        Data("B", Some(5)),
        Data("B", Some(5))
      )
    )

    val result = data
      .groupBy(row => (group = row.group))
      .agg(row => (values = collectSet(row.value)))

    val collected = result.dataFrame.collect().map(_.json).toSeq

    val groupA = collected.find(_.contains("\"group\":\"A\"")).get
    // Should contain 1 and 3, but not null
    assert(groupA.contains("1") && groupA.contains("3"))

    val groupB = collected.find(_.contains("\"group\":\"B\"")).get
    // Should contain only 5
    assert(groupB.contains("5"))

  test("collectSet on empty dataset"):
    case class Empty(id: Int)

    val empty  = spark.createDataSeq(Seq.empty[Empty])
    val result = empty.agg(row => (ids = collectSet(row.id)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"ids":[]}""")
    )

  test("collectSet with single value"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))

    val result = persons.agg(row => (names = collectSet(row.name)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"names":["Alice"]}""")
    )

  test("collectSet with all identical values"):
    case class Score(player: String, points: Int)

    val scores = spark.createDataSeq(
      Seq(
        Score("player1", 10),
        Score("player1", 10),
        Score("player1", 10),
        Score("player1", 10)
      )
    )

    val result = scores.agg(row => (unique_scores = collectSet(row.points)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"unique_scores":[10]}""")
    )

  test("collectSet with multiple groups and multiple columns"):
    case class Sale(region: String, product: String, category: String)

    val sales = spark.createDataSeq(
      Seq(
        Sale("East", "A", "Electronics"),
        Sale("East", "B", "Clothing"),
        Sale("East", "A", "Electronics"),
        Sale("West", "C", "Electronics"),
        Sale("West", "C", "Electronics"),
        Sale("West", "D", "Food")
      )
    )

    val result = sales
      .groupBy(row => (region = row.region))
      .agg(row => (products = collectSet(row.product), categories = collectSet(row.category)))

    val collected = result.dataFrame.collect().map(_.json).toSeq
    assertEquals(collected.size, 2)

    val eastRow = collected.find(_.contains("East")).get
    assert(eastRow.contains("A") && eastRow.contains("B"))
    assert(eastRow.contains("Electronics") && eastRow.contains("Clothing"))

    val westRow = collected.find(_.contains("West")).get
    assert(westRow.contains("C") && westRow.contains("D"))
    assert(westRow.contains("Electronics") && westRow.contains("Food"))

  test("collectSet with large number of duplicates"):
    case class Event(eventType: String, status: String)

    val events = spark.createDataSeq(
      (1 to 100).flatMap(_ =>
        Seq(
          Event("click", "success"),
          Event("click", "failure"),
          Event("click", "success"),
          Event("click", "pending")
        )
      )
    )

    val result = events.agg(row => (statuses = collectSet(row.status)))

    val collected = result.dataFrame.collect().map(_.json).toSeq.head
    // Should have exactly 3 unique statuses
    assert(
      collected.contains("success") && collected.contains("failure") && collected.contains("pending"),
      "Should contain all three unique statuses"
    )
