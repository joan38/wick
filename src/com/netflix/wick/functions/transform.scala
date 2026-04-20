package com.netflix.wick
package functions

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import scala.quoted.*

/** Runs an arbitrary Scala expression against column values using a Spark `ScalaUDF`.
  *
  * The inline body may call `.apply()` on any `Expr` in scope to dereference its value at row-evaluation time. The
  * macro collects those calls, encodes their types, and builds a Catalyst `ScalaUDF` that applies the given Scala
  * function to each row.
  *
  * Prefer the dedicated column operations (arithmetic, string helpers, conditionals, etc.) when they fit — they push
  * down into Catalyst optimizations that Scala UDFs do not benefit from. Use `transform` when you need ad-hoc Scala
  * logic on column values, such as string interpolation that mixes several columns.
  *
  * @param f
  *   an inline function of shape `TransformLabel ?=> R` that dereferences `Expr` values via `.apply()`
  * @tparam R
  *   the return type of the function
  * @return
  *   a LinearExpr[R] whose value is the result of evaluating `f` per row
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  * import com.netflix.wick.functions.transform
  *
  * case class Person(name: String, age: Int)
  *
  * persons.select(person => (formatted = transform(s"\${person.name()}-\${person.age()}")))
  * // Combines two columns into a single formatted String per row
  *   }}}
  */
inline def transform[R](inline f: TransformLabel ?=> R): column.LinearExpr[R] = ${ transformMacro('f) }

private def transformMacro[R: Type](contextFn: Expr[TransformLabel ?=> R])(using Quotes): Expr[column.LinearExpr[R]] =
  import quotes.reflect.*

  val bodyTerm = contextFn.asTerm match
    case Inlined(_, _, Lambda(List(_), body)) => body
    case other => report.errorAndAbort(s"transform: expected a TransformLabel context function, got: ${other.show}")

  // Pass 1: collect all expr() calls in traversal order
  val collected = collection.mutable.ListBuffer[(Term, TypeRepr)]()

  object Collector extends TreeAccumulator[Unit]:
    def foldTree(acc: Unit, tree: Tree)(owner: Symbol): Unit = tree match
      case t @ Apply(Apply(Apply(TypeApply(_, _), List(receiver)), Nil), List(_))
          if receiver.tpe.widen.dealias <:< TypeRepr.of[column.Expr[?]] =>
        collected += ((receiver, t.tpe))
      case _ => foldOverTree(acc, tree)(owner)

  Collector.foldTree((), bodyTerm)(Symbol.spliceOwner)

  val n          = collected.size
  val paramTypes = collected.map(_._2).toList
  val receivers  = collected.map(_._1).toList

  if n == 0 then report.errorAndAbort("transform: no Expr() calls found in the body")

  // Pass 2: build lambda (p0: T0, ...) => rewritten body
  var replaceIdx = 0
  val lambdaTerm = Lambda(
    Symbol.spliceOwner,
    MethodType(paramTypes.indices.map(i => s"p$i").toList)(_ => paramTypes, _ => TypeRepr.of[R]),
    (lambdaOwner, params) =>
      replaceIdx = 0
      val transformer = new TreeMap:
        override def transformTerm(tree: Term)(owner: Symbol): Term = tree match
          case Apply(Apply(Apply(TypeApply(_, _), List(receiver)), Nil), List(_))
              if receiver.tpe.widen.dealias <:< TypeRepr.of[column.Expr[?]] =>
            val idx = replaceIdx
            replaceIdx += 1
            Ref(params(idx).symbol)
          case _ => super.transformTerm(tree)(owner)
      transformer.transformTerm(bodyTerm.changeOwner(lambdaOwner))(lambdaOwner)
  )

  // Summon ExpressionEncoder[t] for each param type, building a List at runtime.
  def buildEncoderList(types: List[(TypeRepr, Int)]): Expr[List[ExpressionEncoder[?]]] =
    types match
      case Nil               => '{ List.empty[ExpressionEncoder[?]] }
      case (pt, idx) :: rest =>
        pt.asType match
          case '[t] =>
            Expr.summon[ExpressionEncoder[t]] match
              case Some(enc) =>
                val tail = buildEncoderList(rest)
                '{ $enc.asInstanceOf[ExpressionEncoder[?]] :: $tail }
              case None => report.errorAndAbort(s"transform: no encoder for p$idx")

  // Extract the underlying Catalyst Expression from each receiver column.
  def buildChildrenList(rs: List[Term]): Expr[List[Expression]] =
    rs match
      case Nil       => '{ List.empty[Expression] }
      case r :: rest =>
        r.tpe.widen.dealias match
          case at @ AppliedType(_, List(valType)) if at <:< TypeRepr.of[column.Expr[?]] =>
            valType.asType match
              case '[v] =>
                val child = '{ ${ r.asExprOf[column.Expr[v]] }.underlying }
                val tail  = buildChildrenList(rest)
                '{ $child :: $tail }
          case _ => report.errorAndAbort("transform: unexpected receiver type")

  val encR         = Expr.summon[ExpressionEncoder[R]].getOrElse(report.errorAndAbort("transform: no encoder for R"))
  val encoderList  = buildEncoderList(paramTypes.zip(paramTypes.indices.toList))
  val childrenList = buildChildrenList(receivers)

  '{
    column.LinearExpr(
      ScalaUDF(
        ${ lambdaTerm.asExpr }.asInstanceOf[AnyRef],
        $encR.objSerializer.dataType,
        $childrenList,
        $encoderList.map(enc => Some(enc)),
        Some($encR)
      )
    )
  }
