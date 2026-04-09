package com.netflix.wick
package operations

import org.apache.spark.sql.{Dataset, Encoder}

extension [T](dataSeq: DataSeq[T]) def dataset(using Encoder[T]): Dataset[T] = dataSeq.dataFrame.as[T]

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def dataset(using Encoder[Joined]): Dataset[Joined] = joinedDataSeq.dataFrame.as[Joined]
