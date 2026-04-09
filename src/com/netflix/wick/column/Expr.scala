package com.netflix.wick
package column

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.*
import scala.NamedTuple.AnyNamedTuple
import scala.NamedTupleDecomposition.DropNames
import scala.Tuple.Union
import scala.annotation.nowarn
import scala.annotation.unchecked.uncheckedVariance

/** A type-safe column representation for a Spark column.
  *
  * Col[T] is the core abstraction for working with columns in DataSeqs. It wraps a Spark Expression while maintaining
  * compile-time type safety. The type parameter T represents the data type of the column's values.
  *
  * This class extends Selectable, enabling dynamic field access using dot notation on nested structures, while
  * preserving type safety through Scala's type system.
  *
  * @param expr
  *   the underlying Spark Expression that represents the column operation
  * @tparam T
  *   the type of values this column contains (String, Int, Boolean, case class, etc.)
  *
  * @example
  *   {{{
  * import com.netflix.wick.{*, given}
  *
  * case class Person(name: String, age: Int)
  * case class Address(street: String, city: String)
  * case class PersonWithAddress(person: Person, address: Address)
  *
  * val data = spark.createDataSeq(Seq(
  *   PersonWithAddress(Person("Alice", age = 30), Address("123 Main St", city = "NYC"))
  * ))
  *
  * // Type-safe column access
  * val nameCol = data.select(row => row.person.name) // Compile-time type safety
  * val ageCol = data.select(row => row.person.age)
  *
  * // Dynamic field access on nested structures
  * val streetCol = data.select(row => row.address.street)
  *
  * // Column operations with type safety
  * val isAdult = data.select(row => row.person.age > 18)
  * val greeting = data.select(row => lit("Hello ") + row.person.name)
  *   }}}
  */
sealed class Expr[+T](val underlying: Expression) extends Selectable:
  @nowarn private type Fields = NamedTuple.Map[NamedTuple.From[T @uncheckedVariance], Expr]
  def selectDynamic(name: String): Any = Expr(UnresolvedExtractValue(underlying, lit(name).underlying))

final class LinearExpr[+T](underlying: Expression) extends Expr[T](underlying):
  @nowarn private type Fields = NamedTuple.Map[NamedTuple.From[T @uncheckedVariance], LinearExpr]
  override def selectDynamic(name: String): Any = LinearExpr(UnresolvedExtractValue(underlying, lit(name).underlying))

final class ScalarExpr[+T](underlying: Expression) extends Expr[T](underlying):
  @nowarn private type Fields = NamedTuple.Map[NamedTuple.From[T @uncheckedVariance], ScalarExpr]
  override def selectDynamic(name: String): Any = ScalarExpr(UnresolvedExtractValue(underlying, lit(name).underlying))

type IsScalarCols[Cols <: AnyNamedTuple] = IsScalarExprs[DropNames[Cols]]
type IsScalarExprs[Exprs <: Tuple]       = Union[Exprs] <:< ScalarExpr[?]
