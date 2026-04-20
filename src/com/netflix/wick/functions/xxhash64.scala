package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](expr: Expr[T])
  /** Computes the 64-bit xxHash of the expression's value.
    *
    * Deterministic — equal inputs always produce equal hashes — but non-cryptographic. Useful for bucketing,
    * partitioning, and deterministic sampling.
    *
    * @return
    *   a LinearExpr[Long] containing the 64-bit hash
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * persons.select(person => (bucket = person.name.xxhash64))
    *   }}}
    */
  def xxhash64: LinearExpr[Long] = LinearExpr(
    spark.sql.functions.xxhash64(new Column(expr.underlying)).expr
  )

extension [T](expr: ScalarExpr[T])
  /** Computes the 64-bit xxHash of a scalar expression's value.
    *
    * @return
    *   a ScalarExpr[Long] containing the 64-bit hash
    */
  def xxhash64: ScalarExpr[Long] = ScalarExpr(
    spark.sql.functions.xxhash64(new Column(expr.underlying)).expr
  )
