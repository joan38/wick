package com.netflix.wick
package functions

import com.netflix.wick.column.lit
import org.apache.spark
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Concat
import scala.annotation.targetName

/** Concatenates multiple strings together into a single string.
  *
  * @return
  *   null if any of the input columns are null.
  */
def concat(exprs: Expr[String]*): LinearExpr[String] = LinearExpr(
  spark.sql.functions.concat(exprs.map(expr => new Column(expr.underlying))*).expr
)

def concat(exprs: ScalarExpr[String]*): ScalarExpr[String] = ScalarExpr(
  spark.sql.functions.concat(exprs.map(expr => new Column(expr.underlying))*).expr
)

extension (string: Expr[String])
  infix def ++(other: Expr[String]): LinearExpr[String] =
    LinearExpr(Concat(Seq(string.underlying, other.underlying)))
  infix def ++(other: String): LinearExpr[String] =
    LinearExpr(Concat(Seq(string.underlying, lit(other).underlying)))

extension (string: ScalarExpr[String])
  infix def ++(other: ScalarExpr[String]): ScalarExpr[String] =
    ScalarExpr(Concat(Seq(string.underlying, other.underlying)))
  infix def ++(other: String): ScalarExpr[String] =
    ScalarExpr(Concat(Seq(string.underlying, lit(other).underlying)))

extension (string: String)
  infix def ++(other: Expr[String]): LinearExpr[String] =
    LinearExpr(Concat(Seq(lit(string).underlying, other.underlying)))
  infix def ++(other: ScalarExpr[String]): ScalarExpr[String] =
    ScalarExpr(Concat(Seq(lit(string).underlying, other.underlying)))
  infix def ++(other: String): ScalarExpr[String] =
    ScalarExpr(Concat(Seq(lit(string).underlying, lit(other).underlying)))

/** Concatenates multiple arrays together into a single array.
  *
  * @return
  *   null if any of the input columns are null.
  */
@targetName("concatArrays")
def concat[T](exprs: Expr[Seq[T]]*): LinearExpr[Seq[T]] = LinearExpr(
  spark.sql.functions.concat(exprs.map(expr => new Column(expr.underlying))*).expr
)

@targetName("concatArrays")
def concat[T](exprs: ScalarExpr[Seq[T]]*): ScalarExpr[Seq[T]] = ScalarExpr(
  spark.sql.functions.concat(exprs.map(expr => new Column(expr.underlying))*).expr
)

extension [T](array: Expr[Seq[T]])
  /** Concatenates multiple arrays together into a single array.
    *
    * @return
    *   null if any of the input columns are null.
    */
  @targetName("concatArrays")
  def ++[U](other: Expr[Seq[T]]): LinearExpr[Seq[U]] = LinearExpr(
    spark.sql.functions
      .concat(new Column(array.underlying), new Column(other.underlying))
      .expr
  )

extension [T](array: ScalarExpr[Seq[T]])
  /** Concatenates multiple arrays together into a single array.
    *
    * @return
    *   null if any of the input columns are null.
    */
  @targetName("concatArrays")
  def ++[U](other: ScalarExpr[Seq[T]]): ScalarExpr[Seq[U]] = ScalarExpr(
    spark.sql.functions
      .concat(new Column(array.underlying), new Column(other.underlying))
      .expr
  )
