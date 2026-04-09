package com.netflix.wick
package operations

extension [T](dataSeq: DataSeq[T]) def limit(n: Int): DataSeq[T] = DataSeq[T](dataSeq.dataFrame.limit(n))

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def limit(n: Int): JoinedDataSeq[Joined] = JoinedDataSeq(joinedDataSeq.dataFrame.limit(n), joinedDataSeq.refs)
