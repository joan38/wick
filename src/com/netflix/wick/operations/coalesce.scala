package com.netflix.wick
package operations

extension [T](dataSeq: DataSeq[T])
  def coalesce(numPartitions: Int): DataSeq[T] = DataSeq[T](dataSeq.dataFrame.coalesce(numPartitions))

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def coalesce(numPartitions: Int): DataSeq[Joined] = DataSeq[Joined](joinedDataSeq.dataFrame.coalesce(numPartitions))
