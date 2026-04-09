package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*
import java.sql.Timestamp
import scala.concurrent.duration.*

class withWatermarkTest extends FunSuite with SparkSuite:

  test("apply watermark with event time column"):
    val events = spark.createDataSeq(
      Seq(
        Event(1, Timestamp.valueOf("2024-01-01 10:00:00"), "event1"),
        Event(2, Timestamp.valueOf("2024-01-01 10:01:00"), "event2"),
        Event(3, Timestamp.valueOf("2024-01-01 10:02:00"), "event3")
      )
    )

    val withWatermark = events.withWatermark(_.eventTime, 10.seconds)

    // Verify the DataFrame is created successfully
    assertEquals(withWatermark.dataFrame.count(), 3L)

    // Verify all data is preserved
    assertEquals(
      withWatermark.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"id":1,"eventTime":"2024-01-01 10:00:00","data":"event1"}""",
        """{"id":2,"eventTime":"2024-01-01 10:01:00","data":"event2"}""",
        """{"id":3,"eventTime":"2024-01-01 10:02:00","data":"event3"}"""
      ).sorted
    )

  test("apply watermark with different delay thresholds"):
    val events = spark.createDataSeq(
      Seq(
        Event(1, Timestamp.valueOf("2024-01-01 10:00:00"), "event1"),
        Event(2, Timestamp.valueOf("2024-01-01 10:01:00"), "event2")
      )
    )

    // Test with minutes
    val withMinutes = events.withWatermark(_.eventTime, 5.minutes)
    assertEquals(withMinutes.dataFrame.count(), 2L)

    // Test with hours
    val withHours = events.withWatermark(_.eventTime, 1.hour)
    assertEquals(withHours.dataFrame.count(), 2L)

    // Test with days
    val withDays = events.withWatermark(_.eventTime, 1.day)
    assertEquals(withDays.dataFrame.count(), 2L)

  test("apply watermark on empty DataSeq"):
    val emptyEvents   = spark.createDataSeq(Seq.empty[Event])
    val withWatermark = emptyEvents.withWatermark(_.eventTime, 10.seconds)
    assertEquals(withWatermark.dataFrame.count(), 0L)

  test("chain watermark with other operations"):
    val events = spark.createDataSeq(
      Seq(
        Event(1, Timestamp.valueOf("2024-01-01 10:00:00"), "event1"),
        Event(2, Timestamp.valueOf("2024-01-01 10:01:00"), "event2"),
        Event(3, Timestamp.valueOf("2024-01-01 10:02:00"), "event3")
      )
    )

    val result = events
      .withWatermark(_.eventTime, 10.seconds)
      .filter(_.id > 1)

    assertEquals(result.dataFrame.count(), 2L)
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"id":2,"eventTime":"2024-01-01 10:01:00","data":"event2"}""",
        """{"id":3,"eventTime":"2024-01-01 10:02:00","data":"event3"}"""
      )
    )

  test("watermark with zero duration"):
    val events = spark.createDataSeq(
      Seq(
        Event(1, Timestamp.valueOf("2024-01-01 10:00:00"), "event1")
      )
    )

    val withWatermark = events.withWatermark(_.eventTime, 0.seconds)
    assertEquals(withWatermark.dataFrame.count(), 1L)

  test("watermark returns DataSeq with same type"):
    val events = spark.createDataSeq(
      Seq(
        Event(1, Timestamp.valueOf("2024-01-01 10:00:00"), "event1")
      )
    )

    val withWatermark: DataSeq[Event] = events.withWatermark(_.eventTime, 10.seconds)

    // Type is preserved, so we can continue to use Event-specific operations
    val filtered = withWatermark.filter(_.id === 1)
    assertEquals(filtered.dataFrame.count(), 1L)
