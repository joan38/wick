package com.netflix.wick.operations

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.SparkSuite
import com.netflix.wick.model.*
import com.netflix.wick.functions.++

class selectTest extends FunSuite with SparkSuite:

  test("selecting columns with transformations"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35))
    )

    val doubleAges = persons.select(person => (double_age = nullable(person.age.? * 2): LinearExpr[Int | Null]))
    assertEquals(doubleAges.dataFrame.count(), 3L)
    assertEquals(doubleAges.dataFrame.schema, StructType(Array(StructField("double_age", IntegerType, true))))
    assertEquals(
      doubleAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"double_age":60}""",
        """{"double_age":30}""",
        """{"double_age":70}"""
      )
    )

    val quadrupleAges = doubleAges.select(row => (quadruple_age = nullable(row.double_age.? * 2)))
    assertEquals(quadrupleAges.dataFrame.count(), 3L)
    assertEquals(quadrupleAges.dataFrame.schema, StructType(Array(StructField("quadruple_age", IntegerType, true))))
    assertEquals(
      quadrupleAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"quadruple_age":120}""",
        """{"quadruple_age":60}""",
        """{"quadruple_age":140}"""
      )
    )

  test("selecting columns that don't exist should fail compilation"):
    compileErrors("""
      val persons =
        spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15), Person("Charlie", age = 35)))

      persons.select(person => (non_existent = person.non_existent_column))
    """)

  test("selecting after a join"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 1),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 2),
        Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 1)
      )
    )

    val empDepts         = employees.join(departments, _.dept_id === _.id)
    val selectedEmpDepts = empDepts.select((emp, dept) =>
      (emp_id = emp.id, emp_name = "Employee: " ++ emp.name, dept_name = dept.name ++ " Department")
    )

    assertEquals(selectedEmpDepts.dataFrame.count(), 3L)
    assertEquals(
      selectedEmpDepts.dataFrame.schema,
      StructType(
        Array(
          StructField("emp_id", IntegerType, true),
          StructField("emp_name", StringType, true),
          StructField("dept_name", StringType, true)
        )
      )
    )
    assertEquals(
      selectedEmpDepts.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"emp_id":1,"emp_name":"Employee: Alice","dept_name":"Engineering Department"}""",
        """{"emp_id":2,"emp_name":"Employee: Bob","dept_name":"Marketing Department"}""",
        """{"emp_id":3,"emp_name":"Employee: Charlie","dept_name":"Engineering Department"}"""
      )
    )
