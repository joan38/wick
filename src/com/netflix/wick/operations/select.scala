package com.netflix.wick
package operations

import com.netflix.wick.column.Columns
import org.apache.spark.sql.Column
import com.netflix.wick.util.NamedTuples.InverseMap
import com.netflix.wick.util.*
import scala.NamedTuple.AnyNamedTuple
import scala.quoted.*

extension [T](dataSeq: DataSeq[T])
  def select[Cols <: AnyNamedTuple: Columns as columns](on: DataSeq.Ref[T] => Cols): DataSeq[Selected[Cols]] =
    val namedExpressions = columns(on(DataSeq.Ref(Some(dataSeq.dataFrame))))
    DataSeq(dataSeq.dataFrame.select(namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))*))

  inline def select[U]: DataSeq[U] = ${ selectMacro[T, U]('dataSeq) }

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def select[Cols <: AnyNamedTuple: Columns as columns](
      on: Tuple.Map[Joined, DataSeq.Ref] => Cols
  ): DataSeq[Selected[Cols]] =
    val namedExpressions = columns(on(joinedDataSeq.refs))
    DataSeq(joinedDataSeq.dataFrame.select(namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))*))

type Selected[Cols <: AnyNamedTuple] = InverseMap[Cols, column.Expr]

private def selectMacro[T: Type, U: Type](using Quotes)(dataSeq: Expr[DataSeq[T]]): Expr[DataSeq[U]] =
  import quotes.reflect.*

  val fromType = TypeRepr.of[T]
  val toType   = TypeRepr.of[U]

  if !isSubsetMatch(fromType, toType) then
    val fromStruct = typeStructure(fromType)
    val toStruct   = typeStructure(toType)

    report.error(
      s"""Type structures do not match for .select[${Type.show[U]}] conversion.
         |
         |Source type ${Type.show[T]}:
         |${fromStruct.map((n, t) => s"  $n: ${t.show}").mkString("\n")}
         |
         |Target type ${Type.show[U]}:
         |${toStruct.map((n, t) => s"  $n: ${t.show}").mkString("\n")}
         |
         |All fields of the target type must exist in the source type with matching types.""".stripMargin
    )

  val toFields = typeStructure(toType)
  if toFields.isEmpty then '{ DataSeq($dataSeq.dataFrame) }
  else
    val fromMap  = typeStructure(fromType).toMap
    val colExprs = buildColumnExprs(fromMap, toFields, "")
    '{
      val cols = $colExprs
      DataSeq($dataSeq.dataFrame.select(cols*))
    }

private def buildColumnExprs(using
    Quotes
)(
    fromMap: Map[String, quotes.reflect.TypeRepr],
    toFields: Seq[(String, quotes.reflect.TypeRepr)],
    prefix: String
): Expr[Seq[org.apache.spark.sql.Column]] =
  val colExprs = toFields.map { (name, toFieldType) =>
    val fromFieldType = fromMap(name)
    val fullPath      = if prefix.isEmpty then name else s"$prefix.$name"
    val pathExpr      = Expr(fullPath)
    val nameExpr      = Expr(name)
    val toFieldStruct = typeStructure(toFieldType)

    (seqElemType(fromFieldType), seqElemType(toFieldType)) match
      case (Some(fromElem), Some(toElem)) if !typesMatch(fromElem, toElem) =>
        val toElemFields = typeStructure(toElem)
        val lambdaExpr   = buildSeqElemLambda(toElemFields)
        '{
          org.apache.spark.sql.functions
            .transform(
              org.apache.spark.sql.functions.col($pathExpr),
              $lambdaExpr
            )
            .as($nameExpr)
        }
      case _ =>
        (mapValueTypes(fromFieldType), mapValueTypes(toFieldType)) match
          case (Some((_, fromVal)), Some((_, toVal))) if !typesMatch(fromVal, toVal) =>
            val toValFields = typeStructure(toVal)
            val lambdaExpr  = buildMapValueLambda(toValFields)
            '{
              org.apache.spark.sql.functions
                .transform_values(
                  org.apache.spark.sql.functions.col($pathExpr),
                  $lambdaExpr
                )
                .as($nameExpr)
            }
          case _ =>
            if toFieldStruct.isEmpty || typesMatch(fromFieldType, toFieldType) then
              '{ org.apache.spark.sql.functions.col($pathExpr).as($nameExpr) }
            else
              val fromSubMap = typeStructure(fromFieldType).toMap
              val subExprs   = buildColumnExprs(fromSubMap, toFieldStruct, fullPath)
              '{
                val subCols = $subExprs
                org.apache.spark.sql.functions.struct(subCols*).as($nameExpr)
              }
  }

  Expr.ofSeq(colExprs)

private def mapValueTypes(using
    Quotes
)(
    tpe: quotes.reflect.TypeRepr
): Option[(quotes.reflect.TypeRepr, quotes.reflect.TypeRepr)] =
  import quotes.reflect.*
  tpe.dealias match
    case AppliedType(map, List(key, value)) if map.typeSymbol == TypeRepr.of[Map[?, ?]].typeSymbol =>
      Some((key, value))
    case _ => None

private def buildMapValueLambda(using
    Quotes
)(
    toValFields: Seq[(String, quotes.reflect.TypeRepr)]
): Expr[(org.apache.spark.sql.Column, org.apache.spark.sql.Column) => org.apache.spark.sql.Column] =
  import quotes.reflect.*
  val colType    = TypeRepr.of[org.apache.spark.sql.Column]
  val lambdaTerm = Lambda(
    Symbol.spliceOwner,
    MethodType(List("k", "v"))(_ => List(colType, colType), _ => colType),
    (methSym, params) =>
      val vExpr      = params(1).asInstanceOf[Term].asExprOf[org.apache.spark.sql.Column]
      val fieldExprs = toValFields.map { (name, _) =>
        val nameExpr = Expr(name)
        '{ $vExpr.getField($nameExpr).as($nameExpr) }
      }
      val fieldSeqExpr = Expr.ofSeq(fieldExprs)
      '{
        org.apache.spark.sql.functions.when(
          $vExpr.isNotNull,
          org.apache.spark.sql.functions.struct($fieldSeqExpr*)
        )
      }.asTerm.changeOwner(methSym)
  )
  lambdaTerm.asExprOf[(org.apache.spark.sql.Column, org.apache.spark.sql.Column) => org.apache.spark.sql.Column]

private def seqElemType(using Quotes)(tpe: quotes.reflect.TypeRepr): Option[quotes.reflect.TypeRepr] =
  import quotes.reflect.*
  tpe.dealias match
    case AppliedType(seq, List(elem)) if seq.typeSymbol == TypeRepr.of[Seq[?]].typeSymbol => Some(elem)
    case _                                                                                => None

private def buildSeqElemLambda(using
    Quotes
)(
    toElemFields: Seq[(String, quotes.reflect.TypeRepr)]
): Expr[org.apache.spark.sql.Column => org.apache.spark.sql.Column] =
  import quotes.reflect.*
  val colType    = TypeRepr.of[org.apache.spark.sql.Column]
  val lambdaTerm = Lambda(
    Symbol.spliceOwner,
    MethodType(List("el"))(_ => List(colType), _ => colType),
    (methSym, params) =>
      val elExpr     = params.head.asInstanceOf[Term].asExprOf[org.apache.spark.sql.Column]
      val fieldExprs = toElemFields.map { (name, _) =>
        val nameExpr = Expr(name)
        '{ $elExpr.getField($nameExpr).as($nameExpr) }
      }
      val fieldSeqExpr = Expr.ofSeq(fieldExprs)
      '{
        org.apache.spark.sql.functions.when(
          $elExpr.isNotNull,
          org.apache.spark.sql.functions.struct($fieldSeqExpr*)
        )
      }.asTerm.changeOwner(methSym)
  )
  lambdaTerm.asExprOf[org.apache.spark.sql.Column => org.apache.spark.sql.Column]
