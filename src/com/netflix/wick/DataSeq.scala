package com.netflix.wick

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import scala.annotation.nowarn

/** A type-safe wrapper around Spark DataFrame providing compile-time safety and IDE support.
  *
  * DataSeq[T] is the core abstraction in Wick that replaces Spark's untyped DataFrame and performance-limited Dataset
  * with a fully type-safe API. It maintains all the performance benefits of DataFrame while providing compile-time
  * guarantees about column names, types, and operations.
  *
  * Unlike Spark's Dataset which can have runtime performance issues due to object serialization, DataSeq operates
  * directly on the underlying DataFrame for optimal performance while preserving type safety through Scala's type
  * system.
  *
  * @param dataFrame
  *   the underlying Spark DataFrame that powers this DataSeq
  * @tparam T
  *   the type representing the schema of this DataSeq (typically a case class)
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  * import org.apache.spark.sql.SparkSession
  *
  * val spark = SparkSession.builder().master("local").getOrCreate()
  *
  * case class Person(name: String, age: Int, city: String)
  * case class Department(id: Int, name: String)
  * case class Employee(id: Int, name: String, dept_id: Int)
  *
  * // Creating DataSeqs
  * val persons = spark.createDataSeq(Seq(
  *   Person("Alice", age = 30, city = "NYC"),
  *   Person("Bob", age = 25, city = "LA"),
  *   Person("Charlie", age = 35, city = "Chicago")
  * ))
  *
  * // Type-safe filtering with compile-time column validation
  * val adults = persons.filter(_.age >= 18)
  * val newYorkers = persons.filter(_.city === "NYC")
  * val youngAdults = persons.filter(person => person.age >= 18 && person.age < 30)
  *
  * // Type-safe selection and transformation
  * val names = persons.select(person => (name = person.name))
  * val doubledAges = persons.select(person => (
  *   name = person.name,
  *   doubled_age = person.age * 2
  * ))
  *
  * // Type-safe joins
  * val departments = spark.createDataSeq(Seq(
  *   Department(1, "Engineering"),
  *   Department(2, "Marketing")
  * ))
  * val employees = spark.createDataSeq(Seq(
  *   Employee(1, "Alice", 1),
  *   Employee(2, "Bob", 2)
  * ))
  *
  * val empDepts = employees.join(departments, _.dept_id === _.id)
  * val result = empDepts.select((emp, dept) => (
  *   emp_name = emp.name,
  *   dept_name = dept.name
  * ))
  *
  * // Type-safe aggregations
  * import com.netflix.wick.functions.{count, `*`}
  * val ageGroups = persons
  *   .groupBy(person => (age_group = if (person.age > 30) "senior" else "junior"))
  *   .agg(group => (population = count(`*`)))
  *
  * // Chaining operations
  * val processed = persons
  *   .filter(_.age >= 25)
  *   .select(person => (name = person.name, category = "adult"))
  *   .count() // Returns Long
  *   }}}
  *
  * @note
  *   DataSeq operations are lazy and will not execute until an action like `count()`, `show()`, or `collect()` is
  *   called. This follows Spark's lazy evaluation model for optimal performance.
  *
  * @see
  *   [[JoinedDataSeq]] for type-safe operations on joined data
  * @see
  *   [[GroupedDataSeq]] for type-safe grouping and aggregation operations
  * @see
  *   [[Expr]] for type-safe column operations and expressions
  */
class DataSeq[T](val dataFrame: DataFrame)

object DataSeq:
  /** A type-safe representation of a Table.
    *
    * Table[T] serves as a type-safe wrapper around Spark DataFrames, enabling compile-time verified column access
    * through Scala's Selectable trait. The type parameter T represents the schema structure, typically a case class
    * that mirrors the DataFrame's column types and names.
    *
    * This abstraction allows accessing DataFrame columns using dot notation (e.g., `table.columnName`) while
    * maintaining full type safety. The optional DataFrame parameter enables both resolved (with actual DataFrame) and
    * unresolved (schema-only) contexts.
    *
    * @param dataFrame
    *   optional DataFrame instance - Some(df) for resolved context with actual data, None for unresolved context used
    *   in schema-only operations
    * @tparam T
    *   the schema type representing the structure of the table, typically a case class where field names match
    *   DataFrame column names and types match column types
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Person(name: String, age: Int, city: String)
    * case class Address(street: String, zipCode: String)
    * case class PersonWithAddress(person: Person, address: Address)
    *
    * val spark = SparkSession.builder().master("local").getOrCreate()
    * val persons = spark.createDataSeq(Seq(
    *   Person("Alice", age = 30, city = "NYC"),
    *   Person("Bob", age = 25, city = "LA")
    * ))
    *
    * // Table is used internally in DataSeq operations to provide type-safe column access
    * val filtered = persons.filter(row => row.age > 25) // row is Table[Person]
    * val selected = persons.select(row => (
    *   name = row.name,        // row.name is Expr[String]
    *   isAdult = row.age >= 18 // row.age is Expr[Int]
    * ))
    *
    * // Works with nested structures
    * val nestedData = spark.createDataSeq(Seq(
    *   PersonWithAddress(Person("Alice", age = 30, city = "NYC"), Address("123 Main St", zipCode = "10001"))
    * ))
    * val streetNames = nestedData.select(row => (street = row.address.street))
    *   }}}
    */
  final case class Ref[T](dataFrame: Option[DataFrame]) extends Selectable:
    @nowarn private type Fields = NamedTuple.Map[NamedTuple.From[T], LinearExpr]
    def selectDynamic(name: String): Any = LinearExpr(dataFrame match
      case Some(df) => df.col(name).expr
      case None     => UnresolvedAttribute.quotedString(name))

/** Converts a Spark Dataset to a type-safe DataSeq.
  *
  * This extension method allows you to wrap existing Spark Datasets with Wick's type-safe DataSeq while maintaining the
  * same underlying data and performance characteristics.
  *
  * @param dataset
  *   the Spark Dataset to convert
  * @tparam T
  *   the type of the Dataset elements
  * @return
  *   a new DataSeq wrapping the provided Dataset
  *
  * @example
  *   {{{
  * val dataset = spark.range(100).map(i => Person(s"person$i", i.toInt))
  * val dataSeq = dataset.toDataSeq
  *   }}}
  */
extension [T](dataset: Dataset[T]) def toDataSeq: DataSeq[T] = DataSeq(dataset.toDF())
