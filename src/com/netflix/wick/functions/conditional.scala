package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

def conditional[Result](
    condition: Expr[Boolean],
    `then`: Expr[Result],
    `else`: Expr[Result]
): LinearExpr[Result] = LinearExpr(
  spark.sql.functions
    .when(new Column(condition.underlying), new Column(`then`.underlying))
    .otherwise(new Column(`else`.underlying))
    .expr
)

def conditional[Result](
    condition: ScalarExpr[Boolean],
    `then`: ScalarExpr[Result],
    `else`: ScalarExpr[Result]
): ScalarExpr[Result] = ScalarExpr(
  spark.sql.functions
    .when(new Column(condition.underlying), new Column(`then`.underlying))
    .otherwise(new Column(`else`.underlying))
    .expr
)
