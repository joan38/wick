package com.netflix.wick

import munit.Suite
import org.apache.spark.sql.SparkSession

trait SparkSuite extends Suite:
  lazy val spark =
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark

  override def afterAll(): Unit = spark.close()
