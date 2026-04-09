package com.netflix.wick.functions

import munit.FunSuite
import org.apache.spark.sql.types.*
import com.netflix.wick.model.*
import com.netflix.wick.SparkSuite
import com.netflix.wick.{*, given}
import com.netflix.wick.functions.asc

class ascTest extends FunSuite with SparkSuite:

  test("sorting by a single column asc"):
    val persons =
      spark.createDataSeq(
        Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35))
      )

    val result = persons.orderBy(person => asc(person.age))
    assertEquals(
      result.dataFrame.collect().map(_.json).toSeq,
      Seq(
        """{"name":"Bob","age":25}""",
        """{"name":"Alice","age":30}""",
        """{"name":"Charlie","age":35}"""
      )
    )

  test("using * in asc as asc(*) should not compile"):
    compileErrors("""
      val persons =
        spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 25), Person("Charlie", age = 35)))

      persons.orderBy(_ => asc(`*`))
    """)

  test("using * in asc as asc(*) should not compile"):
    compileErrors("""
      case class A(name_age: Map[String, Int])

      val multis =
        spark.createDataSeq(Seq(A(Map("Alice" -> 30)), A(Map("Bob" -> 25)), A(Map("Charlie" -> 35))))

      multis.orderBy(a => asc(a.name_age))
    """)
    // Would have resulted in a:
    // org.apache.spark.sql.catalyst.ExtendedAnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "name_age ASC NULLS FIRST" due to data type mismatch: The `sortorder` does not support ordering on type "MAP<STRING, INT>".
