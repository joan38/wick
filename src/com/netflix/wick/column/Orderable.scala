package com.netflix.wick
package column

import java.sql.Timestamp
import java.time.Instant
import scala.annotation.implicitNotFound

/** Type class for types that can be ordered.
  *
  * Map is for example not orderable.
  */
@implicitNotFound("${T} is not orderable. No given Orderable[${T}] found")
class Orderable[T]

object Orderable:
  given [T: Orderable] => Orderable[LinearExpr[T]]      = Orderable()
  given [T: Orderable] => Orderable[T | Null]           = Orderable()
  given [T <: Numeric] => Orderable[T]                  = Orderable()
  given [T: Orderable, Arr <: Seq[T]] => Orderable[Arr] = Orderable()
  given Orderable[String]                               = Orderable()
  given Orderable[Timestamp]                            = Orderable()
  given Orderable[Instant]                              = Orderable()
  given Orderable[Boolean]                              = Orderable()

  /** Orderable for Tuples of Expr[?] that contains only orderable elements (possibly different types) */
  given [Exprs <: Tuple] => (Orderable[Tuple.Head[Exprs]], Orderable[Tuple.Tail[Exprs]]) => Orderable[Exprs] =
    Orderable()
  given [T: Orderable] => Orderable[T *: EmptyTuple] = Orderable()
