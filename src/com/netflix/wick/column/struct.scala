package com.netflix.wick
package column

import com.netflix.wick.operations.Selected
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CreateStruct
import scala.NamedTuple.AnyNamedTuple

/** Creates a struct expression from named tuple fields.
  *
  * This function builds a struct (record/object-like) expression from a collection of named fields. The resulting
  * expression can be either a ScalarExpr or LinearExpr depending on the input field types.
  *
  * @param fields
  *   the named tuple containing the fields to include in the struct
  * @tparam Fields
  *   the type of the named tuple, must extend AnyNamedTuple
  * @return
  *   either a ScalarExpr[Fields] or LinearExpr[Fields] depending on the field types
  */
def struct[Cols <: AnyNamedTuple: {Columns as cols, ExprCreator as exprCreator}](
    fields: Cols
): exprCreator.Exp[Selected[Cols]] =
  val expressions = cols(fields).map(nf => new Column(nf.expr.underlying).as(nf.name).expr)
  exprCreator(CreateStruct.create(expressions))
