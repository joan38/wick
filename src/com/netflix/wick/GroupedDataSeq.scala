package com.netflix.wick

import com.netflix.wick.column.{Columns, IsScalarCols}
import com.netflix.wick.util.NamedTuples.InverseMap
import org.apache.spark.sql.{Column, RelationalGroupedDataset}
import scala.NamedTuple.AnyNamedTuple

class GroupedDataSeq[Keys <: AnyNamedTuple, T](groupedDataset: RelationalGroupedDataset):
  inline def agg[Cols <: AnyNamedTuple: {Columns as cols, IsScalarCols}](
      on: DataSeq.Ref[T] => Cols
  ): DataSeq[Agg[Keys, Cols]] =
    val namedExpressions = cols(on(DataSeq.Ref[T](None)))
    val aggExpressions   = namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))
    DataSeq(groupedDataset.agg(aggExpressions.head, aggExpressions.tail*))
end GroupedDataSeq

class JoinedGroupedDataSeq[Keys <: AnyNamedTuple, Joined <: Tuple](
    groupedDataset: RelationalGroupedDataset,
    tables: Tuple.Map[Joined, DataSeq.Ref]
):
  def agg[Cols <: AnyNamedTuple: {Columns as cols, IsScalarCols}](
      on: Tuple.Map[Joined, DataSeq.Ref] => Cols
  ): DataSeq[Agg[Keys, Cols]] =
    val namedExpressions = cols(on(tables))
    val aggExpressions   = namedExpressions.map(ne => new Column(ne.expr.underlying).as(ne.name))
    DataSeq(groupedDataset.agg(aggExpressions.head, aggExpressions.tail*))
end JoinedGroupedDataSeq

type Agg[Keys <: AnyNamedTuple, Cols <: AnyNamedTuple] =
  NamedTuple.Concat[InverseMap[Keys, Expr], InverseMap[Cols, Expr]]
