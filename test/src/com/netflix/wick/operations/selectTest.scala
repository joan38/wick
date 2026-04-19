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

  test("select from tuples to a case class with same schema"):
    val personsV1 = spark.createDataSeq(Seq(Person("Alice", 30), Person("Bob", 25)))

    val personsV2: DataSeq[Person] = personsV1
      .select(row => (name = row.name, age = row.age))
      .select[Person]

    assertEquals(personsV2.dataFrame.count(), 2L)
    assertEquals(personsV2.dataFrame.columns.toSeq, Seq("name", "age"))

  test("select from tuples to a case class with extra fields"):
    val personsV1 = spark.createDataSeq(Seq(Person("Alice", 30), Person("Bob", 25)))
    val personsV2 = personsV1
      .withColumns(row => (extra = row.name))
      .select[Person]

    assertEquals(personsV2.dataFrame.count(), 2L)
    assertEquals(personsV2.dataFrame.columns.toSeq, Seq("name", "age"))

  test("select from tuples to a case class with extra fields in sub struct"):
    case class Full(name: String, age: Int, attr: FullAttr)
    case class FullAttr(height: Int, extra: String)

    case class Slim(name: String, age: Int, attr: SlimAttr)
    case class SlimAttr(height: Int)

    val personsV1 = spark.createDataSeq(Seq(Full("Alice", 30, FullAttr(178, "extra"))))
    val personsV2 = personsV1.select[Slim]

    assertEquals(personsV2.dataFrame.count(), 1L)
    assertEquals(
      personsV2.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = true),
          StructField("attr", StructType(Array(StructField(name = "height", IntegerType))), nullable = false)
        )
      )
    )

  test("select from tuples to a case class with extra fields in nullable sub struct"):
    case class Full(name: String, age: Int, attr: FullAttr | Null)
    case class FullAttr(height: Int, extra: String)

    case class Slim(name: String, age: Int, attr: SlimAttr | Null)
    case class SlimAttr(height: Int)

    val personsV1 = spark.createDataSeq(Seq(Full("Alice", 30, FullAttr(178, "extra"))))
    val personsV2 = personsV1.select[Slim]

    assertEquals(personsV2.dataFrame.count(), 1L)
    assertEquals(
      personsV2.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = true),
          StructField("attr", StructType(Array(StructField(name = "height", IntegerType))), nullable = false)
        )
      )
    )

  test("select from tuples to a case class with extra fields in array of sub struct"):
    case class Full(name: String, age: Int, attr: Seq[FullAttr])
    case class FullAttr(height: Int, extra: String)

    case class Slim(name: String, age: Int, attr: Seq[SlimAttr])
    case class SlimAttr(height: Int)

    val personsV1 = spark.createDataSeq(Seq(Full("Alice", 30, Seq(FullAttr(178, "extra")))))
    val personsV2 = personsV1.select[Slim]

    assertEquals(personsV2.dataFrame.count(), 1L)
    assertEquals(
      personsV2.dataFrame.schema,
      StructType(
        Array(
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = true),
          StructField(
            "attr",
            ArrayType(StructType(Array(StructField(name = "height", IntegerType))), containsNull = true),
            nullable = true
          )
        )
      )
    )

  test("select from tuples to a case class with extra fields in map of sub struct"):
    case class Full(name: String, age: Int, attrs: Map[String, FullAttr])
    case class FullAttr(height: Int, extra: String)

    case class Slim(name: String, age: Int, attrs: Map[String, SlimAttr])
    case class SlimAttr(height: Int)

    val personsV1 = spark.createDataSeq(Seq(Full("Alice", 30, Map("main" -> FullAttr(178, "extra")))))
    val personsV2 = personsV1.select[Slim]

    assertEquals(personsV2.dataFrame.count(), 1L)
    assertEquals(personsV2.dataFrame.columns.toSeq, Seq("name", "age", "attrs"))
    assertEquals(
      personsV2.dataFrame.collect().map(_.json).toSeq,
      Seq("""{"name":"Alice","age":30,"attrs":{"main":{"height":178}}}""")
    )

  test("select should fail when source is missing a field"):
    case class Slim(name: String, age: Int)
    case class Full(name: String, age: Int, extra: Boolean)

    val slim  = spark.createDataSeq(Seq(Slim("Alice", 30)))
    val error = compileErrors("""slim.select[Full]""")
    assert(clue(error).contains("Type structures do not match"))

  test("select should fail when source is missing a field in sub struct"):
    case class Slim(name: String, age: Int, attr: SlimAttr | Null)
    case class SlimAttr(height: Int)

    case class Full(name: String, age: Int, attr: FullAttr | Null)
    case class FullAttr(height: Int, extra: String)

    val slim  = spark.createDataSeq(Seq(Slim("Alice", 30, SlimAttr(178))))
    val error = compileErrors("""slim.select[Full]""")
    assert(clue(error).contains("Type structures do not match"))

  test("select should fail when source is missing a field in array of sub struct"):
    case class Slim(name: String, age: Int, attr: Seq[SlimAttr])
    case class SlimAttr(height: Int)

    case class Full(name: String, age: Int, attr: Seq[FullAttr])
    case class FullAttr(height: Int, extra: String)

    val slim  = spark.createDataSeq(Seq(Slim("Alice", 30, Seq(SlimAttr(178)))))
    val error = compileErrors("""slim.select[Full]""")
    assert(clue(error).contains("Type structures do not match"))

  test("select should fail when types differ"):
    case class Source(name: String, age: Int)
    case class Target(name: String, age: String)

    val source = spark.createDataSeq(Seq(Source("Alice", 30)))
    val error  = compileErrors("""source.select[Target]""")
    assert(clue(error).contains("Type structures do not match"))
    assert(clue(error).contains("age: scala.Int"))
    assert(clue(error).contains("age: java.lang.String"))
