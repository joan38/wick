package com.netflix.wick
package functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala3encoders.derivation.Deserializer

/** Creates a type-safe user-defined function (UDF) that takes one parameter.
  *
  * @param f
  *   the function to transform values of type P to values of type R
  * @tparam P
  *   the input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from a Expr[P] to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val doubleValue = udf((x: Int) => x * 2)
  * val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15)))
  * val result = persons.select(row => (doubled_age = doubleValue(row.age)))
  *   }}}
  */
def udf[P, R](f: P => R)(using
    ExpressionEncoder[P],
    ExpressionEncoder[R],
    Deserializer[R]
): Expr[P] => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  p => LinearExpr(udf(new Column(p.underlying)).expr)

/** Creates a type-safe user-defined function (UDF) that takes two parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val concatenate = udf((s1: String, s2: String) => s"$s1-$s2")
  * val persons = spark.createDataSeq(Seq(Person("Alice", age = 30), Person("Bob", age = 15)))
  * val result = persons.select(row => (full_info = concatenate(row.name, lit("years"))))
  *   }}}
  */
def udf[P0, P1, R](f: (P0, P1) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1) => LinearExpr(udf(new Column(p0.underlying), new Column(p1.underlying)).expr)

/** Creates a type-safe user-defined function (UDF) that takes three parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam P2
  *   the third input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val combine = udf((name: String, age: Int, active: Boolean) => s"$name-$age-$active")
  * val persons = spark.createDataSeq(Seq(Person("Alice", age = 30, active = true), Person("Bob", age = 15, active = false)))
  * val result = persons.select(row => (combined = combine(row.name, row.age, lit(true))))
  *   }}}
  */
def udf[P0, P1, P2, R](f: (P0, P1, P2) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1], Expr[P2]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2) => LinearExpr(udf(new Column(p0.underlying), new Column(p1.underlying), new Column(p2.underlying)).expr)

/** Creates a type-safe user-defined function (UDF) that takes four parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2, P3) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam P2
  *   the third input parameter type
  * @tparam P3
  *   the fourth input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2], Expr[P3]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int) => a + b * c - d)
  * val data = spark.createDataSeq(Seq((1, 2, 3, 4), (5, 6, 7, 8)))
  * val result = data.select(row => (result = complexCalc(row._1, row._2, row._3, row._4)))
  *   }}}
  */
def udf[P0, P1, P2, P3, R](f: (P0, P1, P2, P3) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1], Expr[P2], Expr[P3]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying)
      ).expr
    )

/** Creates a type-safe user-defined function (UDF) that takes five parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2, P3, P4) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam P2
  *   the third input parameter type
  * @tparam P3
  *   the fourth input parameter type
  * @tparam P4
  *   the fifth input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int) => a + b * c - d + e)
  * val data = spark.createDataSeq(Seq((1, 2, 3, 4, 5), (6, 7, 8, 9, 10)))
  * val result = data.select(row => (result = complexCalc(row._1, row._2, row._3, row._4, row._5)))
  *   }}}
  */
def udf[P0, P1, P2, P3, P4, R](f: (P0, P1, P2, P3, P4) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying)
      ).expr
    )

/** Creates a type-safe user-defined function (UDF) that takes six parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2, P3, P4, P5) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam P2
  *   the third input parameter type
  * @tparam P3
  *   the fourth input parameter type
  * @tparam P4
  *   the fifth input parameter type
  * @tparam P5
  *   the sixth input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5]) to
  *   Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int, f: Int) => a + b * c - d + e * f)
  * val result = data.select(row => (result = complexCalc(row.a, row.b, row.c, row.d, row.e, row.f)))
  *   }}}
  */
def udf[P0, P1, P2, P3, P4, P5, R](f: (P0, P1, P2, P3, P4, P5) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying)
      ).expr
    )

/** Creates a type-safe user-defined function (UDF) that takes seven parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2, P3, P4, P5, P6) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam P2
  *   the third input parameter type
  * @tparam P3
  *   the fourth input parameter type
  * @tparam P4
  *   the fifth input parameter type
  * @tparam P5
  *   the sixth input parameter type
  * @tparam P6
  *   the seventh input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5],
  *   Expr[P6]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int, f: Int, g: Int) => a + b + c + d + e + f + g)
  * val result = data.select(row => (result = complexCalc(row.a, row.b, row.c, row.d, row.e, row.f, row.g)))
  *   }}}
  */
def udf[P0, P1, P2, P3, P4, P5, P6, R](f: (P0, P1, P2, P3, P4, P5, P6) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5], Expr[P6]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying)
      ).expr
    )

/** Creates a type-safe user-defined function (UDF) that takes eight parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2, P3, P4, P5, P6, P7) to values of type R
  * @tparam P0
  *   the first input parameter type
  * @tparam P1
  *   the second input parameter type
  * @tparam P2
  *   the third input parameter type
  * @tparam P3
  *   the fourth input parameter type
  * @tparam P4
  *   the fifth input parameter type
  * @tparam P5
  *   the sixth input parameter type
  * @tparam P6
  *   the seventh input parameter type
  * @tparam P7
  *   the eighth input parameter type
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5],
  *   Expr[P6], Expr[P7]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int, f: Int, g: Int, h: Int) => a + b + c + d + e + f + g + h)
  * val result = data.select(row => (result = complexCalc(row.a, row.b, row.c, row.d, row.e, row.f, row.g, row.h)))
  *   }}}
  */
def udf[P0, P1, P2, P3, P4, P5, P6, P7, R](f: (P0, P1, P2, P3, P4, P5, P6, P7) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[R],
    Deserializer[R]
): (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5], Expr[P6], Expr[P7]) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying)
      ).expr
    )

/** Creates a type-safe user-defined function (UDF) that takes nine parameters.
  *
  * @param f
  *   the function to transform values of type (P0, P1, P2, P3, P4, P5, P6, P7, P8) to values of type R
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from (Expr[P0], Expr[P1], Expr[P2], Expr[P3], Expr[P4], Expr[P5],
  *   Expr[P6], Expr[P7], Expr[P8]) to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int, f: Int, g: Int, h: Int, i: Int) => a + b + c + d + e + f + g + h + i)
  * val result = data.select(row => (result = complexCalc(row.a, row.b, row.c, row.d, row.e, row.f, row.g, row.h, row.i)))
  *   }}}
  */
def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, R](f: (P0, P1, P2, P3, P4, P5, P6, P7, P8) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying)
      ).expr
    )

/** Creates a type-safe user-defined function (UDF) that takes ten parameters.
  *
  * @param f
  *   the function to transform values to values of type R
  * @tparam R
  *   the return type
  * @return
  *   a function that represents a transformation from ten columns to Expr[R]
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * val complexCalc = udf((a: Int, b: Int, c: Int, d: Int, e: Int, f: Int, g: Int, h: Int, i: Int, j: Int) => a + b + c + d + e + f + g + h + i + j)
  * val result = data.select(row => (result = complexCalc(/* 10 columns */)))
  *   }}}
  */
def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, R](f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R](f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => R)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[P16],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15],
    Expr[P16]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying),
        new Column(p16.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[P16],
    ExpressionEncoder[P17],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15],
    Expr[P16],
    Expr[P17]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying),
        new Column(p16.underlying),
        new Column(p17.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[P16],
    ExpressionEncoder[P17],
    ExpressionEncoder[P18],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15],
    Expr[P16],
    Expr[P17],
    Expr[P18]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying),
        new Column(p16.underlying),
        new Column(p17.underlying),
        new Column(p18.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[P16],
    ExpressionEncoder[P17],
    ExpressionEncoder[P18],
    ExpressionEncoder[P19],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15],
    Expr[P16],
    Expr[P17],
    Expr[P18],
    Expr[P19]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying),
        new Column(p16.underlying),
        new Column(p17.underlying),
        new Column(p18.underlying),
        new Column(p19.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[P16],
    ExpressionEncoder[P17],
    ExpressionEncoder[P18],
    ExpressionEncoder[P19],
    ExpressionEncoder[P20],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15],
    Expr[P16],
    Expr[P17],
    Expr[P18],
    Expr[P19],
    Expr[P20]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying),
        new Column(p16.underlying),
        new Column(p17.underlying),
        new Column(p18.underlying),
        new Column(p19.underlying),
        new Column(p20.underlying)
      ).expr
    )

def udf[P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, R](
    f: (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => R
)(using
    ExpressionEncoder[P0],
    ExpressionEncoder[P1],
    ExpressionEncoder[P2],
    ExpressionEncoder[P3],
    ExpressionEncoder[P4],
    ExpressionEncoder[P5],
    ExpressionEncoder[P6],
    ExpressionEncoder[P7],
    ExpressionEncoder[P8],
    ExpressionEncoder[P9],
    ExpressionEncoder[P10],
    ExpressionEncoder[P11],
    ExpressionEncoder[P12],
    ExpressionEncoder[P13],
    ExpressionEncoder[P14],
    ExpressionEncoder[P15],
    ExpressionEncoder[P16],
    ExpressionEncoder[P17],
    ExpressionEncoder[P18],
    ExpressionEncoder[P19],
    ExpressionEncoder[P20],
    ExpressionEncoder[P21],
    ExpressionEncoder[R],
    Deserializer[R]
): (
    Expr[P0],
    Expr[P1],
    Expr[P2],
    Expr[P3],
    Expr[P4],
    Expr[P5],
    Expr[P6],
    Expr[P7],
    Expr[P8],
    Expr[P9],
    Expr[P10],
    Expr[P11],
    Expr[P12],
    Expr[P13],
    Expr[P14],
    Expr[P15],
    Expr[P16],
    Expr[P17],
    Expr[P18],
    Expr[P19],
    Expr[P20],
    Expr[P21]
) => LinearExpr[R] =
  val udf = scala3udf.Udf(f)
  (p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21) =>
    LinearExpr(
      udf(
        new Column(p0.underlying),
        new Column(p1.underlying),
        new Column(p2.underlying),
        new Column(p3.underlying),
        new Column(p4.underlying),
        new Column(p5.underlying),
        new Column(p6.underlying),
        new Column(p7.underlying),
        new Column(p8.underlying),
        new Column(p9.underlying),
        new Column(p10.underlying),
        new Column(p11.underlying),
        new Column(p12.underlying),
        new Column(p13.underlying),
        new Column(p14.underlying),
        new Column(p15.underlying),
        new Column(p16.underlying),
        new Column(p17.underlying),
        new Column(p18.underlying),
        new Column(p19.underlying),
        new Column(p20.underlying),
        new Column(p21.underlying)
      ).expr
    )
