package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension (expr: Expr[String])
  /** Tests whether a string starts with the given prefix.
    *
    * @param prefix
    *   the prefix to test for
    * @return
    *   a LinearExpr[Boolean] that is true when the string starts with the prefix
    */
  def startsWith(prefix: Expr[String]): LinearExpr[Boolean] = LinearExpr(
    spark.sql.functions.startswith(new Column(expr.underlying), new Column(prefix.underlying)).expr
  )

extension (expr: ScalarExpr[String])
  /** Tests whether a scalar string starts with the given prefix.
    *
    * @param prefix
    *   the prefix to test for
    * @return
    *   a ScalarExpr[Boolean] that is true when the string starts with the prefix
    */
  def startsWith(prefix: ScalarExpr[String]): ScalarExpr[Boolean] = ScalarExpr(
    spark.sql.functions.startswith(new Column(expr.underlying), new Column(prefix.underlying)).expr
  )
