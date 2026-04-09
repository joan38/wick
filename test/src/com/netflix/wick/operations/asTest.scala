package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.column.struct
import scala.annotation.nowarn

class asTest extends FunSuite with SparkSuite:

  test("convert between case classes with same structure"):
    case class PersonV1(name: String, age: Int)
    case class PersonV2(name: String, age: Int)

    val personsV1 = spark.createDataSeq(
      Seq(PersonV1("Alice", 30), PersonV1("Bob", 25))
    )

    val personsV2: DataSeq[PersonV2] = personsV1.as[PersonV2]

    assertEquals(personsV2.dataFrame.count(), 2L)
    assertEquals(
      personsV2.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, true),
          StructField("age", IntegerType, true)
        )
      )
    )

  test("convert case class with same nested structure"):
    case class Person(name: String, address: Address)
    case class Address(street: String, city: String)

    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", Address("123 Main St", "NYC")),
        Person("Bob", Address("456 Oak Ave", "LA"))
      )
    )

    val personsConverted: DataSeq[Person] = persons.as[Person]

    assertEquals(personsConverted.dataFrame.count(), 2L)
    assertEquals(
      personsConverted.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, true),
          StructField(
            "address",
            StructType(
              Array(
                StructField("street", StringType, true),
                StructField("city", StringType, true)
              )
            ),
            true
          )
        )
      )
    )

  test("convert named tuples to case class"):
    case class PersonV1(fullName: String, years: Int)
    case class PersonV2(name: String, age: Int)

    val personsV1 = spark.createDataSeq(
      Seq(PersonV1("Alice", 30), PersonV1("Bob", 25))
    )

    val transformed = personsV1.select(p => (name = p.fullName, age = p.years))

    val personsV2: DataSeq[PersonV2] = transformed.as[PersonV2]

    assertEquals(personsV2.dataFrame.count(), 2L)
    val collected = personsV2.dataFrame.collect()
    assertEquals(collected(0).getAs[String]("name"), "Alice")
    assertEquals(collected(0).getAs[Int]("age"), 30)

  test("convert named tuples with nested named tuples to case class"):
    case class Person(name: String, middles: Seq[String], address: Address, attrs: Map[String, Boolean])
    case class Address(street: String, city: String)

    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", Seq("Auburn"), Address("123 Main St", "NYC"), Map("married" -> true)),
        Person("Bob", Seq("Dave"), Address("456 Oak Ave", "LA"), Map.empty)
      )
    )

    val transformed = persons.select(p =>
      (
        name = p.name,
        middles = p.middles,
        address = struct(street = p.address.street, city = p.address.city),
        attrs = p.attrs
      )
    )

    val personsAgain: DataSeq[Person] = transformed.as[Person]
    assertEquals(personsAgain.dataFrame.count(), 2L)

  test("convert named tuples with nested case class to case class"):
    case class Person(name: String, middles: Seq[String], address: Address, attrs: Map[String, Boolean])
    case class Address(street: String, city: String)

    val persons = spark.createDataSeq(
      Seq(
        Person("Alice", Seq("Auburn"), Address("123 Main St", "NYC"), Map("married" -> true)),
        Person("Bob", Seq("Dave"), Address("456 Oak Ave", "LA"), Map.empty)
      )
    )

    val transformed = persons.select(p =>
      (
        name = p.name,
        middles = p.middles,
        address = p.address,
        attrs = p.attrs
      )
    )

    val personsAgain: DataSeq[Person] = transformed.as[Person]
    assertEquals(personsAgain.dataFrame.count(), 2L)

  test("converting to a subtype should be ok"):
    case class Address(street: String, city: String | Null)

    val addresses = spark.createDataSeq(
      Seq(
        Address("123 Main St", "NYC"),
        Address("456 Oak Ave", "LA")
      )
    )

    val transformed = addresses.select(p => (street = p.street, city = lit(null)))

    val addressAgain: DataSeq[Address] = transformed.as[Address]
    assertEquals(addressAgain.dataFrame.count(), 2L)

  test("converting the wrong types should not compile"):
    case class Address(street: String, city: String)

    val addresses = spark.createDataSeq(
      Seq(
        Address("123 Main St", "NYC"),
        Address("456 Oak Ave", "LA")
      )
    )

    @nowarn val transformed = addresses.select(p => (street = p.street, city = lit(1)))

    val error = compileErrors("""transformed.as[Address]""")

    assert(error.contains("Type structures do not match"))
    assert(error.contains("city: scala.Int"))
    assert(error.contains("city: java.lang.String"))
