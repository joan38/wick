package com.netflix.wick
package operations

import org.apache.spark.storage.StorageLevel

extension [T](dataSeq: DataSeq[T])
  def persist(level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataSeq[T] =
    DataSeq(dataSeq.dataFrame.persist(level))
