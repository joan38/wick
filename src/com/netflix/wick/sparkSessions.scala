package com.netflix.wick

import org.apache.spark.sql.{Encoder, SparkSession}
import scala.annotation.targetName

extension (spark: SparkSession)
  /** Creates a DataSeq from a Scala sequence of case class instances.
    *
    * This method converts a regular Scala Seq into a type-safe DataSeq, enabling all of Wick's compile-time safety
    * features. The case class structure will be automatically mapped to the DataFrame schema.
    *
    * @param data
    *   the sequence of case class instances to convert into a DataSeq
    * @tparam T
    *   the case class type representing the schema, must have an implicit Encoder
    * @return
    *   a new DataSeq containing the provided data
    *
    * @example
    *   {{{
    * case class Person(name: String, age: Int)
    * val persons = spark.createDataSeq(Seq(
    *   Person("Alice", age = 30),
    *   Person("Bob", age = 15)
    * ))
    *   }}}
    */
  def createDataSeq[T: Encoder](data: Seq[T]): DataSeq[T] = DataSeq[T](spark.createDataset(data).toDF())

  /** Creates a DataSeq from an existing table in the Spark catalog.
    *
    * This method loads data from a table (Iceberg, Hive, etc.) that exists in the Spark catalog and wraps it in a
    * type-safe DataSeq. The table schema must match the provided type T.
    *
    * @param tableName
    *   the name of the table to load from the Spark catalog
    * @tparam T
    *   the case class type representing the expected schema of the table
    * @return
    *   a new DataSeq backed by the specified table
    *
    * @note
    *   This method assumes the table schema matches the case class structure. Runtime errors may occur if there's a
    *   mismatch between the actual table schema and the expected type T.
    *
    * @example
    *   {{{
    * case class Employee(id: Int, name: String, dept_id: Int)
    * val employees = spark.tableDataSeq[Employee]("hr.employees")
    *   }}}
    */
  def loadTable[T](tableName: String): DataSeq[T] = DataSeq[T](spark.table(tableName))

  @targetName("loadTableType")
  def loadTable[T: TableCoordinate as coordinate]: DataSeq[T] = DataSeq[T](spark.table(coordinate))
