package com.netflix.wick
package operations

extension [T](dataSeq: DataSeq[T])
  def show(): Unit                                = dataSeq.dataFrame.show()
  def show(numRows: Int): Unit                    = dataSeq.dataFrame.show(numRows)
  def show(numRows: Int, truncate: Boolean): Unit = dataSeq.dataFrame.show(numRows, truncate)
  def show(numRows: Int, truncate: Int): Unit     = dataSeq.dataFrame.show(numRows, truncate)

extension [Joined <: Tuple](joinedDataSeq: JoinedDataSeq[Joined])
  def show(): Unit                                = joinedDataSeq.dataFrame.show()
  def show(numRows: Int): Unit                    = joinedDataSeq.dataFrame.show(numRows)
  def show(numRows: Int, truncate: Boolean): Unit = joinedDataSeq.dataFrame.show(numRows, truncate)
  def show(numRows: Int, truncate: Int): Unit     = joinedDataSeq.dataFrame.show(numRows, truncate)
