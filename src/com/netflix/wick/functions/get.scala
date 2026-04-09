package com.netflix.wick
package functions

import com.netflix.wick.column.lit
import org.apache.spark
import org.apache.spark.sql.Column
import scala.compiletime.error

extension [T](array: Expr[Seq[T]])
  /** Retrieves the element at the specified position in the array.
    *
    * @param position
    *   the dynamic position expression (1-based indexing)
    * @return
    *   a LinearExpr containing the element at the given position
    */
  def get(position: Expr[Int]): LinearExpr[T | Null] = LinearExpr(
    spark.sql.functions
      .try_element_at(new Column(array.underlying), new Column(position.underlying))
      .expr
  )

  /** Retrieves the element at the specified position in the array.
    *
    * @param position
    *   the literal position (1-based indexing)
    * @return
    *   a LinearExpr containing the element at the given position
    */
  inline def get(position: Int): LinearExpr[T | Null] =
    if position == 0 then error("SQL array indices start at 1")
    LinearExpr(
      spark.sql.functions
        .try_element_at(new Column(array.underlying), new Column(lit(position).underlying))
        .expr
    )
