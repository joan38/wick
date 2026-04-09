package com.netflix.wick
package operations

import com.netflix.wick.DataSeq
import com.netflix.wick.util.*
import scala.quoted.*

extension [T](dataSeq: DataSeq[T]) inline def as[U]: DataSeq[U] = ${ asMacro[T, U]('dataSeq) }

private def asMacro[T: Type, U: Type](using Quotes)(dataSeq: Expr[DataSeq[T]]): Expr[DataSeq[U]] =
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

  '{ DataSeq($dataSeq.dataFrame) }
