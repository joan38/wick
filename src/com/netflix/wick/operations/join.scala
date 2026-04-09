package com.netflix.wick
package operations

import org.apache.spark.sql.Column
import scala.Tuple.:*

extension [T](dataSeq: DataSeq[T])
  def join[U](other: DataSeq[U], on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]): JoinedDataSeq[(T, U)] =
    innerJoin(other, on)

  def innerJoin[U](other: DataSeq[U], on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]): JoinedDataSeq[(T, U)] =
    val joinOn = on(DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    JoinedDataSeq(
      dataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "inner"),
      (DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    )

  def leftJoin[U](other: DataSeq[U], on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]): JoinedDataSeq[(T, U | Null)] =
    val joinOn = on(DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    JoinedDataSeq(
      dataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "left"),
      (DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    )

  def rightJoin[U](
      other: DataSeq[U],
      on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]
  ): JoinedDataSeq[(T | Null, U)] =
    val joinOn = on(DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    JoinedDataSeq(
      dataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "right"),
      (DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    )

  def outerJoin[U](
      other: DataSeq[U],
      on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]
  ): JoinedDataSeq[(T | Null, U | Null)] =
    val joinOn = on(DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    JoinedDataSeq(
      dataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "outer"),
      (DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    )

  def semiJoin[U](other: DataSeq[U], on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]): DataSeq[T] =
    val joinOn = on(DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    DataSeq(dataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "semi"))

  def antiJoin[U](other: DataSeq[U], on: (DataSeq.Ref[T], DataSeq.Ref[U]) => Expr[?]): DataSeq[T] =
    val joinOn = on(DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    DataSeq(dataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "anti"))

  def crossJoin[U](other: DataSeq[U]): JoinedDataSeq[(T | Null, U | Null)] =
    JoinedDataSeq(
      dataSeq.dataFrame.crossJoin(other.dataFrame),
      (DataSeq.Ref(Some(dataSeq.dataFrame)), DataSeq.Ref(Some(other.dataFrame)))
    )

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def join[U](other: DataSeq[U], on: Tuple.Map[Joined :* U, DataSeq.Ref] => Expr[?]): JoinedDataSeq[Joined :* U] =
    innerJoin(other, on)

  def innerJoin[U](
      other: DataSeq[U],
      on: Tuple.Map[Joined :* U, DataSeq.Ref] => Expr[?]
  ): JoinedDataSeq[Joined :* U] =
    val joinedTables =
      (joinedDataSeq.refs :* DataSeq.Ref(Some(other.dataFrame))).asInstanceOf[Tuple.Map[Joined :* U, DataSeq.Ref]]
    val joinOn = on(joinedTables)
    JoinedDataSeq(
      joinedDataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "inner"),
      joinedTables
    )

  def leftJoin[U](
      other: DataSeq[U],
      on: Tuple.Map[Joined :* U, DataSeq.Ref] => Expr[?]
  ): JoinedDataSeq[Joined :* (U | Null)] =
    val joinedTables =
      (joinedDataSeq.refs :* DataSeq.Ref(Some(other.dataFrame))).asInstanceOf[Tuple.Map[Joined :* U, DataSeq.Ref]]
    val joinOn = on(joinedTables)
    JoinedDataSeq(
      joinedDataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "left"),
      joinedTables.asInstanceOf[Tuple.Map[Joined :* (U | Null), DataSeq.Ref]]
    )

  def semiJoin[U](other: DataSeq[U], on: Tuple.Map[Joined :* U, DataSeq.Ref] => Expr[?]): DataSeq[Joined] =
    val joinedTables =
      (joinedDataSeq.refs :* DataSeq.Ref(Some(other.dataFrame))).asInstanceOf[Tuple.Map[Joined :* U, DataSeq.Ref]]
    val joinOn = on(joinedTables)
    DataSeq(joinedDataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "semi"))

  def antiJoin[U](other: DataSeq[U], on: Tuple.Map[Joined :* U, DataSeq.Ref] => Expr[?]): DataSeq[Joined] =
    val joinedTables =
      (joinedDataSeq.refs :* DataSeq.Ref(Some(other.dataFrame))).asInstanceOf[Tuple.Map[Joined :* U, DataSeq.Ref]]
    val joinOn = on(joinedTables)
    DataSeq(joinedDataSeq.dataFrame.join(other.dataFrame, new Column(joinOn.underlying), "anti"))
