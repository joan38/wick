package com.netflix.wick

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import scala.NamedTuple.NamedTuple
import scala3encoders.derivation.Serializer
import scala3encoders.derivation.Deserializer

inline given [N <: Tuple, V <: Tuple: Serializer as serializer] => Serializer[NamedTuple[N, V]]:
  def inputType: DataType                            = serializer.inputType
  def serialize(inputObject: Expression): Expression = serializer.serialize(inputObject)

inline given [N <: Tuple, V <: Tuple: Deserializer as deserializer] => Deserializer[NamedTuple[N, V]]:
  def inputType: DataType                       = deserializer.inputType
  def deserialize(path: Expression): Expression = deserializer.deserialize(path)
  override def nullable: Boolean                = false

/** Serializer for nullable values */
given [T: Serializer as serializer] => Serializer[T | Null]:
  def inputType: DataType                            = serializer.inputType
  def serialize(inputObject: Expression): Expression = serializer.serialize(inputObject)

/** Deserializer for nullable values */
given [T: Deserializer as deserializer] => Deserializer[T | Null]:
  def inputType: DataType                       = deserializer.inputType
  def deserialize(path: Expression): Expression = deserializer.deserialize(path)
  override def nullable: Boolean                = true
