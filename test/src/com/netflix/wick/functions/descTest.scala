package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.functions.desc

class descTest extends FunSuite with SparkSuite:

  test("sorting by a single column desc"):
    val persons = spark.createDataSeq(
      Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
    )

    val result = persons.orderBy(person => desc(person.age))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Charlie","age":35}""",
        """{"name":"Alice","age":30}""",
        """{"name":"Bob","age":25}"""
      )
    )

  test("using * in desc as desc(*) should not compile"):
    compileErrors("""
      import com.netflix.wick.functions.`*`

      val persons = spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

      persons.orderBy(_ => desc(`*`))
    """)
