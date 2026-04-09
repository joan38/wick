package com.netflix.wick
package operations

extension [T](dataSeq: DataSeq[T])
  def unpersist(blocking: Boolean): DataSeq[T] =
    DataSeq(dataSeq.dataFrame.unpersist(blocking))
