package com.netflix.wick

opaque type TableCoordinate[T] <: String = String

def TableCoordinate[T](coordinate: String): TableCoordinate[T] = coordinate
