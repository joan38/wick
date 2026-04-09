package com.netflix.wick
package operations

import org.apache.spark.sql.Column

extension [T](dataSeq: DataSeq[T])
  def dropDuplicates(on: DataSeq.Ref[T] => Expr[?]): DataSeq[T] =
    val dedupKey = "__deduplicate_key__"
    val dedupOn  = on(DataSeq.Ref(Some(dataSeq.dataFrame)))
    DataSeq(
      dataSeq.dataFrame.withColumn(dedupKey, new Column(dedupOn.underlying)).dropDuplicates(dedupKey).drop(dedupKey)
    )
