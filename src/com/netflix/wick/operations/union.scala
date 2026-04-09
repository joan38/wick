package com.netflix.wick
package operations

extension [T](dataSeq: DataSeq[T])
  def union(other: DataSeq[T]): DataSeq[T] =
    DataSeq[T](dataSeq.dataFrame.union(other.dataFrame))
