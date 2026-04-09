package com.netflix.wick
package operations

import org.apache.spark.sql.Encoder

extension [T: Encoder](dataSeq: DataSeq[T]) def collect(): Array[T] = dataSeq.dataset.collect()
