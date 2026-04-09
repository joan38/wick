package com.netflix.wick.functions

import com.netflix.wick.column.lit
import com.netflix.wick.functions.`*`
import com.netflix.wick.functions.count
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import munit.FunSuite
import org.apache.spark.sql.types.*

class udfTest extends FunSuite with SparkSuite:

  test("single parameter UDF - doubling integers"):
    val doubleValue = udf((x: Int) => x * 2)
    val persons     = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person => (name = person.name, doubled_age = doubleValue(person.age.orElse(0))))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"name":"Alice","doubled_age":60}""",
        """{"name":"Bob","doubled_age":50}"""
      )
    )

  test("single parameter UDF - string transformation"):
    val upperCase = udf((s: String) => s.toUpperCase)
    val persons   = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person => (upper_name = upperCase(person.name), age = person.age))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"upper_name":"ALICE","age":30}""",
        """{"upper_name":"BOB","age":25}"""
      )
    )

  test("two parameter UDF - string concatenation"):
    val concatenate = udf((s1: String, s2: String) => s"$s1-$s2")
    val persons     = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person => (full_info = concatenate(person.name, lit("years"))))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"full_info":"Alice-years"}""",
        """{"full_info":"Bob-years"}"""
      )
    )

  test("two parameter UDF - mathematical operations"):
    val addValues = udf((x: Int, y: Int) => x + y)
    val persons   = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result =
      persons.select(person => (name = person.name, age_plus_ten = addValues(person.age.orElse(0), lit(10))))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"name":"Alice","age_plus_ten":40}""",
        """{"name":"Bob","age_plus_ten":35}"""
      )
    )

  test("three parameter UDF - complex string formatting"):
    val formatInfo = udf((name: String, age: Int, active: Boolean) => s"$name-$age-$active")
    val persons    = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person => (formatted = formatInfo(person.name, person.age.orElse(0), lit(true))))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"formatted":"Alice-30-true"}""",
        """{"formatted":"Bob-25-true"}"""
      )
    )

  test("four parameter UDF - complex calculation"):
    val complexCalc = udf((a: Int, b: Int, c: Int, d: Int) => a + b * c - d)
    case class Numbers(a: Int, b: Int, c: Int, d: Int)
    val numbers = spark.createDataSeq(Seq(Numbers(1, 2, 3, 4), Numbers(5, 6, 7, 8)))

    val result = numbers.select(row => (result = complexCalc(row.a, row.b, row.c, row.d)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"result":39}""", // 5 + 6 * 7 - 8 = 39
        """{"result":3}"""   // 1 + 2 * 3 - 4 = 3
      )
    )

  test("UDF with null handling"):
    val safeDouble = udf((x: Option[Int]) => x.map(_ * 2))
    case class PersonNullable(name: String, age: Option[Int])
    val persons = spark.createDataSeq(
      Seq(
        PersonNullable("Alice", Some(30)),
        PersonNullable("Bob", None),
        PersonNullable("Charlie", Some(25))
      )
    )

    val result = persons.select(person => (name = person.name, doubled_age = safeDouble(person.age)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"name":"Alice","doubled_age":60}""",
        """{"name":"Bob","doubled_age":null}""",
        """{"name":"Charlie","doubled_age":50}"""
      )
    )

  test("UDF returning boolean"):
    val isAdult = udf((age: Int) => age >= 18)
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15)))

    val result = persons.select(person => (name = person.name, adult = isAdult(person.age.orElse(0))))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"name":"Alice","adult":true}""",
        """{"name":"Bob","adult":false}"""
      )
    )

  test("UDF in filter operations"):
    val isNameLongEnough = udf((name: String) => name.length >= 5)
    val persons          = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.filter(person => isNameLongEnough(person.name) === true)
    assertEquals(result.count(), 2L) // Alice and Charlie
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("UDF in aggregation"):
    val ageCategory = udf((age: Int) => if age >= 30 then "senior" else "junior")
    val persons     = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons
      .groupBy(person => (category = ageCategory(person.age.orElse(0))))
      .agg(_ => (count = count(`*`)))

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"category":"junior","count":1}""",
        """{"category":"senior","count":2}"""
      )
    )

  test("five parameter UDF"):
    val fiveParamCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int) => a + b * c - d + e)
    case class FiveNumbers(a: Int, b: Int, c: Int, d: Int, e: Int)
    val numbers = spark.createDataSeq(Seq(FiveNumbers(1, 2, 3, 4, 5)))

    val result = numbers.select(row => (result = fiveParamCalc(row.a, row.b, row.c, row.d, row.e)))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"result":8}""") // 1 + 2 * 3 - 4 + 5 = 8
    )
