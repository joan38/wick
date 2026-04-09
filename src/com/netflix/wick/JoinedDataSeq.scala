package com.netflix.wick

import org.apache.spark.sql.DataFrame

class JoinedDataSeq[Joined <: Tuple](val dataFrame: DataFrame, val refs: Tuple.Map[Joined, DataSeq.Ref])
