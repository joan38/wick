package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.column.lit
import com.netflix.wick.functions.count

class equalityTest extends FunSuite with SparkSuite:

  test("LinearExpr equality operator === with Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons.select(person => (is_alice = person.name === person.name))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("is_alice", BooleanType, nullable = false)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_alice":true}""",
        """{"is_alice":true}""",
        """{"is_alice":true}"""
      )
    )

  test("LinearExpr equality operator === with literal value"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons.select(person => (is_alice = person.name === "Alice"))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("is_alice", BooleanType, nullable = false)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_alice":true}""",
        """{"is_alice":false}""",
        """{"is_alice":false}"""
      )
    )

  test("LinearExpr inequality operator !== with Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons.select(person => (not_same = person.name !== person.name))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("not_same", BooleanType, nullable = false)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"not_same":false}""",
        """{"not_same":false}""",
        """{"not_same":false}"""
      )
    )

  test("LinearExpr inequality operator !== with literal value"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val result = persons.select(person => (not_alice = person.name !== "Alice"))

    assertEquals(
      result.dataFrame.schema,
      StructType(Array(StructField("not_alice", BooleanType, nullable = false)))
    )
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"not_alice":false}""",
        """{"not_alice":true}""",
        """{"not_alice":true}"""
      )
    )

  test("ScalarExpr equality operator === with Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (has_count_one = count(person.name) === 1))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("has_count_one", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"has_count_one":true}""",
        """{"age_group":30,"has_count_one":false}"""
      )
    )

  test("ScalarExpr equality operator === with ScalarExpr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (same_count = count(person.name) === count(person.age)))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("same_count", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"same_count":true}""",
        """{"age_group":30,"same_count":true}"""
      )
    )

  test("ScalarExpr equality operator === with literal value"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (has_count_two = count(person.name) === 2))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("has_count_two", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"has_count_two":false}""",
        """{"age_group":30,"has_count_two":true}"""
      )
    )

  test("ScalarExpr inequality operator !== with Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (not_one = count(person.name) !== 1))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("not_one", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"not_one":false}""",
        """{"age_group":30,"not_one":true}"""
      )
    )

  test("ScalarExpr inequality operator !== with ScalarExpr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (diff_count = count(person.name) !== count(person.age)))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("diff_count", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"diff_count":false}""",
        """{"age_group":30,"diff_count":false}"""
      )
    )

  test("ScalarExpr inequality operator !== with literal value"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (not_two = count(person.name) !== 2))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("not_two", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"not_two":true}""",
        """{"age_group":30,"not_two":false}"""
      )
    )

  test("=== compares column to literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val isThirty = persons.select(person => (is_thirty = person.age === 30))

    assertEquals(
      isThirty.dataFrame.schema,
      StructType(Array(StructField("is_thirty", BooleanType, nullable = false)))
    )
    assertEquals(
      isThirty.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_thirty":true}""",
        """{"is_thirty":false}""",
        """{"is_thirty":true}"""
      )
    )

  test("!== compares column to literal"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val notThirty = persons.select(person => (not_thirty = person.age !== 30))

    assertEquals(
      notThirty.dataFrame.schema,
      StructType(Array(StructField("not_thirty", BooleanType, nullable = false)))
    )
    assertEquals(
      notThirty.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"not_thirty":false}""",
        """{"not_thirty":true}""",
        """{"not_thirty":true}"""
      )
    )

  test("=== on string column"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Alice", age = 35))
    )

    val isAlice = persons.select(person => (is_alice = person.name === "Alice"))

    assertEquals(
      isAlice.dataFrame.schema,
      StructType(Array(StructField("is_alice", BooleanType, nullable = false)))
    )
    assertEquals(
      isAlice.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"is_alice":true}""",
        """{"is_alice":false}""",
        """{"is_alice":true}"""
      )
    )

  test("=== is null-safe (null column equals itself)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val selfEqual = persons.select(person => (self_equal = person.age === person.age))

    assertEquals(
      selfEqual.dataFrame.schema,
      StructType(Array(StructField("self_equal", BooleanType, nullable = false)))
    )
    assertEquals(
      selfEqual.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"self_equal":true}""",
        """{"self_equal":true}""",
        """{"self_equal":true}"""
      )
    )

  test("!== is null-safe (null column does not differ from itself)"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = null), Person("Charlie", age = 35))
    )

    val selfNotEqual = persons.select(person => (self_not_equal = person.age !== person.age))

    assertEquals(
      selfNotEqual.dataFrame.schema,
      StructType(Array(StructField("self_not_equal", BooleanType, nullable = false)))
    )
    assertEquals(
      selfNotEqual.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"self_not_equal":false}""",
        """{"self_not_equal":false}""",
        """{"self_not_equal":false}"""
      )
    )

  test("=== used in join condition"):
    val departments = spark.createDataSeq(
      Seq(
        Department(id = 1, name = "Engineering"),
        Department(id = 2, name = "Marketing")
      )
    )
    val employees = spark.createDataSeq(
      Seq(
        Employee(id = 1, name = "Alice", dept_id = 1, title_id = 1),
        Employee(id = 2, name = "Bob", dept_id = 2, title_id = 1),
        Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 1)
      )
    )

    val empDepts = employees.join(departments, _.dept_id === _.id)

    assertEquals(empDepts.dataFrame.count(), 3L)

  test("=== should not compile when comparing different types"):
    val error = compileErrors("""
      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25))
      )
      persons.select(person => (result = person.name === person.age))
      persons.select(person => (result = person.age === person.name))
    """)
    assertEquals(
      error,
      """error:
        |NamedTuple.NamedTuple[Tuple1[("result" : String)], V] is not a NamedTuple of Expr[?].
        |I found:
        |
        |    com.netflix.wick.column.Columns.given_Columns_NamedTuple[
        |      Tuple1[("result" : String)], V](
        |      /* missing */summon[Tuple.Union[V] <:< com.netflix.wick.column.Expr[?]])
        |
        |But no implicit values were found that match type Tuple.Union[V] <:< com.netflix.wick.column.Expr[?]
        |
        |where:    V is a type variable with constraint >: Tuple1[T1] and <: Tuple
        |.
        |      persons.select(person => (result = person.age === person.name))
        |                                                                    ^
        |error:
        |Cannot compare types Int | Null and String. No implicit Eq[Int | Null, String] found.
        |I found:
        |
        |    com.netflix.wick.column.Eq.given_Eq_A_A[A]
        |
        |But given instance given_Eq_A_A in object Eq does not match type com.netflix.wick.column.Eq[Int | Null, String].
        |      persons.select(person => (result = person.age === person.name))
        |                                                                  ^
        |error:
        |NamedTuple.NamedTuple[Tuple1[("result" : String)], V] is not a NamedTuple of Expr[?].
        |I found:
        |
        |    com.netflix.wick.column.Columns.given_Columns_NamedTuple[
        |      Tuple1[("result" : String)], V](
        |      /* missing */summon[Tuple.Union[V] <:< com.netflix.wick.column.Expr[?]])
        |
        |But no implicit values were found that match type Tuple.Union[V] <:< com.netflix.wick.column.Expr[?]
        |
        |where:    V is a type variable with constraint >: Tuple1[T1] and <: Tuple
        |.
        |      persons.select(person => (result = person.name === person.age))
        |                                                                    ^
        |error:
        |Cannot compare types String and Int | Null. No implicit Eq[String, Int | Null] found.
        |I found:
        |
        |    com.netflix.wick.column.Eq.given_Eq_A_A[A]
        |
        |But given instance given_Eq_A_A in object Eq does not match type com.netflix.wick.column.Eq[String, Int | Null].
        |      persons.select(person => (result = person.name === person.age))
        |                                                                  ^""".stripMargin
    )

  test("=== used in filter"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val thirtyYearOlds = persons
      .filter(person => lit(30) === person.age)
      .filter(person => person.age === lit(30))
      .select(person => (name = person.name))

    assertEquals(
      thirtyYearOlds.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Alice"}""",
        """{"name":"Charlie"}"""
      )
    )
