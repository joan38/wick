package com.netflix.wick
package functions

import com.netflix.wick.column.lit
import org.apache.spark
import org.apache.spark.sql.Column
import scala.compiletime.error
import scala.annotation.{implicitNotFound, targetName}

extension [T](array: Expr[Seq[T]])
  /** Retrieves the element at the specified position in the array.
    *
    * @param position
    *   the dynamic position expression (1-based indexing)
    * @return
    *   a LinearExpr containing the element at the given position
    */
  @targetName("elementAt")
  def apply(position: Expr[Int]): LinearExpr[T] = LinearExpr(
    spark.sql.functions
      .element_at(new Column(array.underlying), new Column(position.underlying))
      .expr
  )

  /** Retrieves the element at the specified position in the array.
    *
    * @param position
    *   the literal position (1-based indexing)
    * @return
    *   a LinearExpr containing the element at the given position
    */
  inline def apply(position: Int): LinearExpr[T] =
    if position == 0 then error("SQL array indices start at 1")
    LinearExpr(
      spark.sql.functions
        .element_at(new Column(array.underlying), new Column(lit(position).underlying))
        .expr
    )

extension [T](array: ScalarExpr[Seq[T]])
  /** Retrieves the element at the specified position in the array.
    *
    * @param position
    *   the dynamic position expression (1-based indexing)
    * @return
    *   a LinearExpr containing the element at the given position
    */
  @targetName("elementAt")
  def apply(position: ScalarExpr[Int]): ScalarExpr[T] = ScalarExpr(
    spark.sql.functions
      .element_at(new Column(array.underlying), new Column(position.underlying))
      .expr
  )

  /** Retrieves the element at the specified position in the array.
    *
    * @param position
    *   the literal position (1-based indexing)
    * @return
    *   a LinearExpr containing the element at the given position
    */
  inline def apply(position: Int): ScalarExpr[T] =
    if position == 0 then error("SQL array indices start at 1")
    ScalarExpr(
      spark.sql.functions
        .element_at(new Column(array.underlying), new Column(lit(position).underlying))
        .expr
    )

extension [K, V](map: Expr[Map[K, V]])
  /** Retrieves the value associated with the specified key in the map.
    *
    * @param key
    *   the dynamic key expression
    * @return
    *   a LinearExpr containing the value associated with the given key
    */
  def apply(key: Expr[K]): LinearExpr[V | Null] = LinearExpr(
    new Column(map.underlying)(new Column(key.underlying)).expr
  )

  /** Retrieves the value associated with the specified key in the map.
    *
    * @param key
    *   the literal key
    * @return
    *   a LinearExpr containing the value associated with the given key
    */
  def apply(key: K): LinearExpr[V | Null] = LinearExpr(
    new Column(map.underlying)(key).expr
  )

extension [K, V](map: ScalarExpr[Map[K, V]])
  /** Retrieves the value associated with the specified key in the map.
    *
    * @param key
    *   the dynamic key expression
    * @return
    *   a LinearExpr containing the value associated with the given key
    */
  def apply(key: Expr[K]): ScalarExpr[V | Null] = ScalarExpr(
    new Column(map.underlying)(new Column(key.underlying)).expr
  )

  /** Retrieves the value associated with the specified key in the map.
    *
    * @param key
    *   the literal key
    * @return
    *   a LinearExpr containing the value associated with the given key
    */
  def apply(key: K): ScalarExpr[V | Null] = ScalarExpr(
    new Column(map.underlying)(key).expr
  )

extension [T](expr: Expr[T])
  def apply()(using TransformLabel): T = throw RuntimeException(
    "This function should only be called within a transform block and will be rewritten its the macro."
  )

@implicitNotFound("Expressions can only be extracted within a transform block")
class TransformLabel
