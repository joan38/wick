package com.netflix.wick
package util

import scala.NamedTuple.{AnyNamedTuple, DropNames, NamedTuple, Names}

private[wick] object NamedTuples:
  type InverseMap[X <: AnyNamedTuple, F[_]] =
    NamedTuple[Names[X], Tuple.InverseMap[DropNames[X], F]]

  type FromRec[T] = T match
    case Seq[?]  => T
    case Product => NamedTuple.Map[NamedTuple.From[T], [t] =>> FromRec[t]]
    case _       => T
