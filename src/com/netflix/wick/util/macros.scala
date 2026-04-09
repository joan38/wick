package com.netflix.wick
package util

import scala.quoted.*

/** Compare two type structures recursively */
private[wick] def typesMatch(using Quotes)(from: quotes.reflect.TypeRepr, to: quotes.reflect.TypeRepr): Boolean =
  import quotes.reflect.*

  // If types are exactly equal, they match
  if from <:< to then return true

  // Check if both are Seq/List types with matching element types
  (from.dealias, to.dealias) match
    case (AppliedType(col1, List(elem1)), AppliedType(col2, List(elem2)))
        if col1.typeSymbol == TypeRepr.of[Seq[?]].typeSymbol
          && col2.typeSymbol == TypeRepr.of[Seq[?]].typeSymbol =>
      return typesMatch(elem1, elem2)
    case _ =>

  // Get structures
  val struct1 = typeStructure(from)
  val struct2 = typeStructure(to)

  // Must have same number of fields
  if struct1.length != struct2.length then return false

  // If both types have no fields (primitives, etc), they must be exactly equal
  if struct1.isEmpty && struct2.isEmpty then return from <:< to

  // Check each field
  struct1.zip(struct2).forall { case ((name1, type1), (name2, type2)) =>
    name1 == name2 && typesMatch(type1, type2)
  }

/** Helper to extract structure from a type (recursively) */
private[wick] def typeStructure(using Quotes)(tpe: quotes.reflect.TypeRepr): List[(String, quotes.reflect.TypeRepr)] =
  import quotes.reflect.*

  val unwrapped = unwrapExprs(tpe)
  unwrapped.dealias match
    // Handle NamedTuple types
    case AppliedType(namedTuple, List(names, values))
        if namedTuple.typeSymbol == TypeRepr.of[NamedTuple.NamedTuple].typeSymbol =>
      extractNamedTupleFields(names, values)

    // Handle case classes and product types
    case _ =>
      unwrapped.typeSymbol.primaryConstructor match
        case constr: Symbol =>
          constr.paramSymss.flatten.collect {
            case p if p.isValDef =>
              p.name -> unwrapExprs(unwrapped.memberType(p))
          }

/** Unwrap NamedTuple value types and any Expr[T] subtypes to their underlying T */
private[wick] def unwrapExprs(using Quotes)(tpe: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
  import quotes.reflect.*

  // Detect any 1-arg type alias that expands to NamedTuple (e.g. Selected[X]).
  // Selected[X] expands to NamedTuple[Names[X], InverseMap[DropNames[X], Expr]], but Names and
  // InverseMap may not reduce via .dealias in macro context. If the alias argument is itself a
  // concrete NamedTuple, use it directly instead of going through the stuck expansion.
  tpe match
    case AppliedType(alias, List(concreteNT))
        if alias.typeSymbol != TypeRepr.of[NamedTuple.NamedTuple].typeSymbol
          && tpe.dealias.typeSymbol == TypeRepr.of[NamedTuple.NamedTuple].typeSymbol =>
      concreteNT.dealias match
        case AppliedType(nt, _) if nt.typeSymbol == TypeRepr.of[NamedTuple.NamedTuple].typeSymbol =>
          return unwrapExprs(concreteNT)
        case _ =>
    case _ =>

  tpe.dealias match
    // Handle NamedTuple — recursively unwrap value types
    case AppliedType(namedTuple, List(names, values))
        if namedTuple.typeSymbol == TypeRepr.of[NamedTuple.NamedTuple].typeSymbol =>
      val unwrappedValues = unwrapTupleValues(values)
      AppliedType(namedTuple, List(names, unwrappedValues))

    // Handle cons *:[head, tail]
    case AppliedType(cons, List(head, tail)) if cons.typeSymbol == TypeRepr.of[*:[?, ?]].typeSymbol =>
      AppliedType(cons, List(unwrapExprs(head), unwrapExprs(tail)))

    // Handle any Expr[T] subtype (LinearExpr, ScalarExpr, Expr itself) — extract T
    case at @ AppliedType(_, List(valueType)) if at <:< TypeRepr.of[column.Expr[?]] =>
      unwrapExprs(valueType)

    case t => t

/** Helper to recursively unwrap Expr types in a tuple values type */
private[wick] def unwrapTupleValues(using Quotes)(tpe: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
  import quotes.reflect.*

  tpe.dealias match
    case AppliedType(cons, List(head, tail)) if cons.typeSymbol == TypeRepr.of[*:[?, ?]].typeSymbol =>
      AppliedType(cons, List(unwrapExprs(head), unwrapTupleValues(tail)))
    case t if t.typeSymbol == TypeRepr.of[EmptyTuple].typeSymbol =>
      t
    case AppliedType(tupleN, args) if isTupleNSymbol(tupleN.typeSymbol) =>
      AppliedType(tupleN, args.map(unwrapExprs))
    case single =>
      unwrapExprs(single)

private[wick] def extractNamedTupleFields(using
    Quotes
)(
    names: quotes.reflect.TypeRepr,
    values: quotes.reflect.TypeRepr
): List[(String, quotes.reflect.TypeRepr)] =
  import quotes.reflect.*

  names match
    case AppliedType(tupleType, nameArgs) if isTupleNSymbol(tupleType.typeSymbol) =>
      val namesList  = nameArgs.collect { case ConstantType(StringConstant(name)) => name }
      val valuesList = flattenTupleValues(values)
      namesList.zip(valuesList).map((name, value) => name -> unwrapExprs(value))

    case _ =>
      (names.dealias, values.dealias) match
        case (ConstantType(StringConstant(name)), value) =>
          List(name -> unwrapExprs(value))

        case (
              AppliedType(namesCons, List(ConstantType(StringConstant(name)), rest)),
              AppliedType(valuesCons, List(value, restValues))
            )
            if namesCons.typeSymbol == TypeRepr.of[*:[?, ?]].typeSymbol
              && valuesCons.typeSymbol == TypeRepr.of[*:[?, ?]].typeSymbol =>
          (name -> unwrapExprs(value)) :: extractNamedTupleFields(rest, restValues)

        case (t, _) if t.typeSymbol == TypeRepr.of[EmptyTuple].typeSymbol => Nil
        case _                                                            => Nil

/** Helper to flatten tuple values from cons form or TupleN form */
private[wick] def flattenTupleValues(using Quotes)(tpe: quotes.reflect.TypeRepr): List[quotes.reflect.TypeRepr] =
  import quotes.reflect.*

  tpe.dealias match
    case AppliedType(cons, List(head, tail)) if cons.typeSymbol == TypeRepr.of[*:[?, ?]].typeSymbol =>
      head :: flattenTupleValues(tail)
    case AppliedType(tupleType, args) if isTupleNSymbol(tupleType.typeSymbol) =>
      args
    case t if t.typeSymbol == TypeRepr.of[EmptyTuple].typeSymbol =>
      Nil
    case single =>
      List(single)

private[wick] def isTupleNSymbol(using Quotes)(sym: quotes.reflect.Symbol): Boolean =
  import quotes.reflect.*
  (1 to 22).exists(n => sym == defn.TupleClass(n))
