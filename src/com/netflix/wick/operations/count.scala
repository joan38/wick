package com.netflix.wick
package operations

extension [T](dataSeq: DataSeq[T]) def count(): Long = dataSeq.dataFrame.count()

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined]) def count(): Long = joinedDataSeq.dataFrame.count()
