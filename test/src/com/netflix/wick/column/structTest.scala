package com.netflix.wick.column

import com.netflix.wick.{*, given}
import com.netflix.wick.functions.*
import com.netflix.wick.functions.`*`
import com.netflix.wick.functions.count
import com.netflix.wick.model.*
import munit.FunSuite
import org.apache.spark.sql.types.*

class structTest extends FunSuite with SparkSuite:

  test("creating simple struct from columns"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person =>
      (
        person_info = struct((name = person.name, age = person.age)),
        original_name = person.name
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"person_info":{"name":"Alice","age":30},"original_name":"Alice"}""",
        """{"person_info":{"name":"Bob","age":25},"original_name":"Bob"}"""
      )
    )

  test("creating struct with mixed data types"):
    case class Employee(id: Int, name: String, salary: Double, active: Boolean)
    val employees = spark.createDataSeq(
      Seq(
        Employee(1, "Alice", 75000.0, true),
        Employee(2, "Bob", 65000.0, false)
      )
    )

    val result = employees.select(emp =>
      (
        employee_details = struct(
          (
            id = emp.id,
            name = emp.name,
            salary = emp.salary,
            is_active = emp.active
          )
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"employee_details":{"id":1,"name":"Alice","salary":75000.0,"is_active":true}}""",
        """{"employee_details":{"id":2,"name":"Bob","salary":65000.0,"is_active":false}}"""
      )
    )

  test("creating nested structs"):
    case class PersonWithAddress(id: Int, name: String, street: String, city: String, zipCode: String)

    val people = spark.createDataSeq(
      Seq(
        PersonWithAddress(1, "Alice", "123 Main St", "NYC", "10001"),
        PersonWithAddress(2, "Bob", "456 Oak Ave", "LA", "90210")
      )
    )

    val result = people.select(person =>
      (
        person_data = struct(
          (
            id = person.id,
            personal_info = struct(
              (
                name = person.name
              )
            ),
            address = struct(
              (
                street = person.street,
                city = person.city,
                zip = person.zipCode
              )
            )
          )
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"person_data":{"id":1,"personal_info":{"name":"Alice"},"address":{"street":"123 Main St","city":"NYC","zip":"10001"}}}""",
        """{"person_data":{"id":2,"personal_info":{"name":"Bob"},"address":{"street":"456 Oak Ave","city":"LA","zip":"90210"}}}"""
      )
    )

  test("struct with computed columns"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15)))

    val result = persons.select(person =>
      (
        person_analysis = struct(
          (
            name = person.name,
            age = person.age,
            is_adult = person.age.orElse(0) >= 18,
            age_category = when(default = lit("junior"), (person.age.orElse(0) >= 30) -> lit("senior"))
          )
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"person_analysis":{"name":"Alice","age":30,"is_adult":true,"age_category":"senior"}}""",
        """{"person_analysis":{"name":"Bob","age":15,"is_adult":false,"age_category":"junior"}}"""
      )
    )

  test("struct with null values"):
    case class PersonNullable(name: String, age: Option[Int], email: Option[String])
    val persons = spark.createDataSeq(
      Seq(
        PersonNullable("Alice", Some(30), Some("alice@example.com")),
        PersonNullable("Bob", None, None),
        PersonNullable("Charlie", Some(25), None)
      )
    )

    val result = persons.select(person =>
      (
        contact_info = struct(
          (
            name = person.name,
            age = person.age,
            email = person.email
          )
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"contact_info":{"name":"Alice","age":30,"email":"alice@example.com"}}""",
        """{"contact_info":{"name":"Bob","age":null,"email":null}}""",
        """{"contact_info":{"name":"Charlie","age":25,"email":null}}"""
      )
    )

  test("struct in groupBy"):
    case class Sales(rep: String, region: String, amount: Double)
    val sales = spark.createDataSeq(
      Seq(
        Sales("Alice", "North", 1000.0),
        Sales("Bob", "South", 1500.0),
        Sales("Alice", "North", 2000.0),
        Sales("Charlie", "North", 1200.0)
      )
    )

    val result = sales
      .groupBy(sale =>
        (
          rep_region = struct(
            (
              rep = sale.rep,
              region = sale.region
            )
          )
        )
      )
      .agg(_ => (total_sales = count(`*`)))

    assertEquals(result.dataFrame.count(), 3L) // Alice-North, Bob-South, Charlie-North

  test("struct in aggregation"):
    case class Sales(rep: String, region: String, amount: Double)
    val sales = spark.createDataSeq(
      Seq(
        Sales("Alice", "North", 1000.0),
        Sales("Bob", "South", 1500.0),
        Sales("Alice", "North", 2000.0),
        Sales("Charlie", "North", 1200.0)
      )
    )

    val result = sales
      .groupBy(sale => (rep = sale.rep))
      .agg(_ => (totals = struct((sales = count(`*`)))))

    assertEquals(result.dataFrame.count(), 3L) // Alice-North, Bob-South, Charlie-North

  test("struct with UDF"):
    val formatPerson = udf((name: String, age: Int) => s"$name ($age years old)")
    val persons      = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person =>
      (
        person_struct = struct(
          (
            name = person.name,
            age = person.age,
            formatted = formatPerson(person.name, person.age.orElse(0))
          )
        )
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"person_struct":{"name":"Alice","age":30,"formatted":"Alice (30 years old)"}}""",
        """{"person_struct":{"name":"Bob","age":25,"formatted":"Bob (25 years old)"}}"""
      )
    )

  test("accessing struct fields after creation"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    // First create the struct
    val withStruct = persons.select(person =>
      (
        person_info = struct((name = person.name, age = person.age)),
        id = lit(1)
      )
    )

    // Then access fields from the struct
    val result = withStruct.select(row =>
      (
        extracted_name = row.person_info.name,
        extracted_age = row.person_info.age,
        id = row.id
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"extracted_name":"Alice","extracted_age":30,"id":1}""",
        """{"extracted_name":"Bob","extracted_age":25,"id":1}"""
      )
    )

  test("struct with single field"):
    val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25)))

    val result = persons.select(person =>
      (
        name_struct = struct((name = person.name))
      )
    )

    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq.sorted,
      Seq(
        """{"name_struct":{"name":"Alice"}}""",
        """{"name_struct":{"name":"Bob"}}"""
      )
    )

  test("empty struct should not compile"):
    compileErrors("""
      val persons = spark.createDataSeq(Seq(Person("Alice", age = 30)))
      persons.select(person => (empty_struct = struct(())))
    """)
