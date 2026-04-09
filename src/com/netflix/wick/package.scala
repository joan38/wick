package com.netflix.wick

// Here we export the main Wick API components for easier access with a single import {*, given}

// Encoders
export scala3encoders.encoder

// Cols
export column.Expr
export column.LinearExpr
export column.ScalarExpr
export column.===
export column.!==
export column.unary_!
export column.&&
export column.||
export column.+
export column.-
export column.`*`
export column./
export column.%
export column.<
export column.<=
export column.>
export column.>=
export column.as
export column.nullable
export column.isNull
export column.isNotNull
export column.orElse
export column.?

// DataSeq operations
export operations.agg
export operations.coalesce
export operations.collect
export operations.count
export operations.dataset
export operations.filter
export operations.groupBy
export operations.join
export operations.innerJoin
export operations.leftJoin
export operations.rightJoin
export operations.outerJoin
export operations.semiJoin
export operations.antiJoin
export operations.crossJoin
export operations.limit
export operations.select
export operations.show
export operations.as
export operations.orderBy
export operations.withColumns
export operations.withWatermark
export operations.writeTo
export operations.persist
//export operations.withColumns

// Enable Numeric for nullable types
given [T: Numeric as numeric] => Numeric[T | Null] = numeric.asInstanceOf[Numeric[T | Null]]
