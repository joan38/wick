package com.netflix.wick
package operations

import org.apache.spark.sql.DataFrameWriterV2

extension [T](dataSeq: DataSeq[T])
  def writeTo(table: String): DataFrameWriterV2[T] =
    // Casting to DataFrameWriterV2[T] instead of creating via dataset to avoid the need for an Encoder[T]
    dataSeq.dataFrame.writeTo(table).asInstanceOf[DataFrameWriterV2[T]]
