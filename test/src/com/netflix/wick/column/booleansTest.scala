package com.netflix.wick.column

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.{*, given}
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.functions.count
import com.netflix.wick.functions.first
import com.netflix.wick.functions.sum

class booleansTest extends FunSuite with SparkSuite:

  test("negation operator !"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val notYoung = persons.select(person => (not_young = !nullable(person.age.? < 30).orElse(false)))

    assertEquals(
      notYoung.dataFrame.schema,
      StructType(Array(StructField("not_young", BooleanType, nullable = false)))
    )
    assertEquals(
      notYoung.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"not_young":true}""",
        """{"not_young":false}""",
        """{"not_young":true}"""
      )
    )

  test("logical AND operator &&"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val middleAged =
      persons.select(person => (middle_aged = nullable((person.age.? >= 25) && (person.age.? <= 30)).orElse(false)))

    assertEquals(
      middleAged.dataFrame.schema,
      StructType(Array(StructField("middle_aged", BooleanType, nullable = false)))
    )
    assertEquals(
      middleAged.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"middle_aged":true}""",
        """{"middle_aged":true}""",
        """{"middle_aged":false}"""
      )
    )

  test("logical OR operator ||"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val youngOrOld = persons.select(person =>
      (young_or_old = nullable(person.age.? < 30).orElse(false) || nullable(person.age.? > 30).orElse(false))
    )

    assertEquals(
      youngOrOld.dataFrame.schema,
      StructType(Array(StructField("young_or_old", BooleanType, nullable = false)))
    )
    assertEquals(
      youngOrOld.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"young_or_old":false}""",
        """{"young_or_old":true}""",
        """{"young_or_old":true}"""
      )
    )

  test("complex boolean expressions"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val complex = persons.select(person =>
      (result =
        !nullable(person.age.? < 25).orElse(false) &&
          nullable(person.age.? <= 30).orElse(false) || nullable(person.age.? > 34).orElse(false)
      )
    )

    assertEquals(
      complex.dataFrame.schema,
      StructType(Array(StructField("result", BooleanType, nullable = false)))
    )
    assertEquals(
      complex.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"result":true}""",
        """{"result":true}""",
        """{"result":true}"""
      )
    )

  test("ScalarExpr negation operator !"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (has_multiple = !(count(person.name) === lit(1L))))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("has_multiple", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"has_multiple":false}""",
        """{"age_group":30,"has_multiple":true}"""
      )
    )

  test("ScalarExpr logical AND with Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (result = count(person.name) > lit(1) && nullable(first(person.age).? >= 30).orElse(false)))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("result", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"result":false}""",
        """{"age_group":30,"result":true}"""
      )
    )

  test("ScalarExpr logical AND with ScalarExpr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (result = count(person.name) > lit(1) && nullable(sum(person.age).? > lit(50)).orElse(false)))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("result", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"result":false}""",
        """{"age_group":30,"result":true}"""
      )
    )

  test("ScalarExpr logical OR with Expr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (result = count(person.name) > lit(1) || nullable(first(person.age).? < lit(30)).orElse(false)))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("result", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"result":true}""",
        """{"age_group":30,"result":true}"""
      )
    )

  test("ScalarExpr logical OR with ScalarExpr"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 30))
    )

    val groupedAges = persons
      .groupBy(person => (age_group = person.age))
      .agg(person => (result = count(person.name) > lit(2) || nullable(sum(person.age).? > lit(50)).orElse(false)))

    assertEquals(
      groupedAges.dataFrame.schema,
      StructType(
        Array(
          StructField("age_group", IntegerType, nullable = true),
          StructField("result", BooleanType, nullable = false)
        )
      )
    )
    assertEquals(
      groupedAges.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"age_group":25,"result":false}""",
        """{"age_group":30,"result":true}"""
      )
    )
