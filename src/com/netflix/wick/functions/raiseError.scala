package com.netflix.wick
package functions

import com.netflix.wick.column.lit
import org.apache.spark
import org.apache.spark.sql.Column

def raiseError(errorMessage: Expr[String]): LinearExpr[Nothing] = LinearExpr(
  spark.sql.functions.raise_error(new Column(errorMessage.underlying)).expr
)

def raiseError(errorMessage: ScalarExpr[String]): ScalarExpr[Nothing] = ScalarExpr(
  spark.sql.functions.raise_error(new Column(errorMessage.underlying)).expr
)

def raiseError(errorMessage: String): ScalarExpr[Nothing] = ScalarExpr(
  spark.sql.functions.raise_error(new Column(lit(errorMessage).underlying)).expr
)
