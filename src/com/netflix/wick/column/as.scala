package com.netflix.wick
package column

import com.netflix.wick.util.*
import scala.quoted.*

extension [T](expr: column.Expr[T]) inline def as[U]: LinearExpr[U] = ${ exprAsMacro[T, U]('expr) }

private def exprAsMacro[T: Type, U: Type](using Quotes)(expr: Expr[column.Expr[T]]): Expr[LinearExpr[U]] =
  validateTypes[T, U]
  '{ LinearExpr[U]($expr.underlying) }

extension [T](expr: ScalarExpr[T]) inline def as[U]: ScalarExpr[U] = ${ scalarExprAsMacro[T, U]('expr) }

private def scalarExprAsMacro[T: Type, U: Type](using Quotes)(expr: Expr[ScalarExpr[T]]): Expr[ScalarExpr[U]] =
  validateTypes[T, U]
  '{ ScalarExpr[U]($expr.underlying) }

private def validateTypes[T: Type, U: Type](using Quotes): Unit =
  import quotes.reflect.*

  val fromType = TypeRepr.of[T]
  val toType   = TypeRepr.of[U]
  if !typesMatch(fromType, toType) then
    val fromStruct = typeStructure(fromType)
    val toStruct   = typeStructure(toType)
    report.error(
      s"""Type structures do not match for .as[${Type.show[U]}] conversion.
         |
         |Source type ${Type.show[T]}:
         |${fromStruct.map((n, t) => s"  $n: ${t.show}").mkString("\n")}
         |
         |Target type ${Type.show[U]}:
         |${toStruct.map((n, t) => s"  $n: ${t.show}").mkString("\n")}
         |
         |Field names and types must match exactly for the conversion to be valid.""".stripMargin
    )
