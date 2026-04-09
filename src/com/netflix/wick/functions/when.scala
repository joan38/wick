package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

def when[Result](
    default: Expr[Result],
    cases: (condition: Expr[Boolean], result: Expr[Result])*
): LinearExpr[Result] =
  val first =
    spark.sql.functions.when(new Column(cases.head.condition.underlying), new Column(cases.head.result.underlying))
  LinearExpr(
    cases.tail
      .foldLeft(first)((acc, `case`) =>
        acc.when(new Column(`case`.condition.underlying), new Column(`case`.result.underlying))
      )
      .otherwise(new Column(default.underlying))
      .expr
  )

def when[Result](
    default: ScalarExpr[Result],
    cases: (condition: Expr[Boolean], result: ScalarExpr[Result])*
): ScalarExpr[Result] =
  val first =
    spark.sql.functions.when(new Column(cases.head.condition.underlying), new Column(cases.head.result.underlying))
  ScalarExpr(
    cases.tail
      .foldLeft(first)((acc, `case`) =>
        acc.when(new Column(`case`.condition.underlying), new Column(`case`.result.underlying))
      )
      .otherwise(new Column(default.underlying))
      .expr
  )
